package io.pixelsdb.spark.merge;

import io.pixelsdb.spark.benchmark.BenchmarkTableDefinition;
import io.pixelsdb.spark.benchmark.BenchmarkTableRegistry;
import io.pixelsdb.spark.hudi.PixelsHudiPartitioning;
import io.pixelsdb.spark.source.PixelsSourceOptions;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

public final class PixelsDeltaMergeJob
{
    private static final Logger LOG = LoggerFactory.getLogger(PixelsDeltaMergeJob.class);

    private PixelsDeltaMergeJob()
    {
    }

    public static StreamingQuery start(SparkSession spark, PixelsDeltaMergeOptions options) throws Exception
    {
        Dataset<Row> source = spark.readStream()
                .format("pixels")
                .option(PixelsSourceOptions.HOST, options.getRpcHost())
                .option(PixelsSourceOptions.PORT, String.valueOf(options.getRpcPort()))
                .option(PixelsSourceOptions.DATABASE, options.getDatabase())
                .option(PixelsSourceOptions.TABLE, options.getTable())
                .option(PixelsSourceOptions.BENCHMARK, options.getBenchmark())
                .option(PixelsSourceOptions.BUCKETS, joinBuckets(options.getBuckets()))
                .option(PixelsSourceOptions.METADATA_HOST, options.getMetadataHost())
                .option(PixelsSourceOptions.METADATA_PORT, String.valueOf(options.getMetadataPort()))
                .load();

        return source.writeStream()
                .outputMode("append")
                .option("checkpointLocation", options.getCheckpointLocation())
                .trigger(buildTrigger(options))
                .foreachBatch(new MergeForeachBatch(options))
                .start();
    }

    public static void processBatch(Dataset<Row> batch, Long batchId, PixelsDeltaMergeOptions options)
    {
        if (batch.isEmpty())
        {
            return;
        }

        if (!options.hasBucketSpecificSinkMode() || !hasBucketColumn(batch.schema()))
        {
            processSingleBatch(batch, batchId, options);
            return;
        }

        List<Row> bucketRows = batch.select(PixelsDeltaMergeColumns.BUCKET_ID).distinct().collectAsList();
        for (Row bucketRow : bucketRows)
        {
            Object rawBucket = bucketRow.get(0);
            if (!(rawBucket instanceof Number))
            {
                continue;
            }

            int bucketId = ((Number) rawBucket).intValue();
            String effectiveSinkMode = options.sinkModeForBucket(bucketId);
            PixelsDeltaMergeOptions bucketOptions = Objects.equals(effectiveSinkMode, options.getSinkMode())
                    ? options
                    : options.withSinkMode(effectiveSinkMode);
            LOG.info("streamingBucketDispatch table={}.{} batchId={} bucket={} sinkMode={}",
                    options.getDatabase(),
                    options.getTable(),
                    batchId,
                    bucketId,
                    effectiveSinkMode);
            Dataset<Row> bucketBatch = batch.filter(batch.col(PixelsDeltaMergeColumns.BUCKET_ID).equalTo(bucketId));
            processSingleBatch(bucketBatch, batchId, bucketOptions);
        }
    }

    private static Trigger buildTrigger(PixelsDeltaMergeOptions options)
    {
        if ("available-now".equalsIgnoreCase(options.getTriggerMode()))
        {
            return Trigger.AvailableNow();
        }
        if ("once".equalsIgnoreCase(options.getTriggerMode()))
        {
            return Trigger.Once();
        }
        return Trigger.ProcessingTime(options.getTriggerInterval());
    }

    private static String joinBuckets(List<Integer> buckets)
    {
        if (buckets == null || buckets.isEmpty())
        {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < buckets.size(); i++)
        {
            if (i > 0)
            {
                builder.append(',');
            }
            builder.append(buckets.get(i));
        }
        return builder.toString();
    }

    private static void processSingleBatch(Dataset<Row> batch, Long batchId, PixelsDeltaMergeOptions options)
    {
        if (batch.isEmpty())
        {
            return;
        }

        PixelsSourceOptions sourceOptions = new PixelsSourceOptions(new LinkedHashMap<String, String>()
        {{
            put(PixelsSourceOptions.HOST, options.getRpcHost());
            put(PixelsSourceOptions.PORT, String.valueOf(options.getRpcPort()));
            put(PixelsSourceOptions.DATABASE, options.getDatabase());
            put(PixelsSourceOptions.TABLE, options.getTable());
            put(PixelsSourceOptions.BENCHMARK,
                    options.getBenchmark() != null ? options.getBenchmark() : BenchmarkTableRegistry.BENCHMARK_HYBENCH);
            put(PixelsSourceOptions.METADATA_HOST, options.getMetadataHost());
            put(PixelsSourceOptions.METADATA_PORT, String.valueOf(options.getMetadataPort()));
        }});

        BenchmarkTableDefinition definition = BenchmarkTableRegistry.require(
                sourceOptions.getBenchmark(),
                options.getTable());
        if (definition.getPrimaryKeyColumns().isEmpty())
        {
            throw new IllegalStateException("Table " + options.getDatabase() + "." + options.getTable()
                    + " does not have a primary key in benchmark definition");
        }

        SparkSession spark = batch.sparkSession();
        List<String> primaryKeys = definition.getPrimaryKeyColumns();
        String mergeCondition = PixelsMergeSupport.buildMergeCondition(primaryKeys);
        if (options.isNoopSinkMode())
        {
            PixelsDeltaPreparedBatch preparedBatch = PixelsDeltaPreparedBatchFactory.prepare(batch, primaryKeys, options);
            try
            {
                new PixelsNoopSinkHandler().handle(
                        preparedBatch,
                        new PixelsDeltaMergeContext(
                                spark,
                                options,
                                batch.schema(),
                                primaryKeys,
                                mergeCondition),
                        batchId);
            }
            finally
            {
                preparedBatch.close();
            }
            return;
        }

        StructType targetSchema = PixelsMergeSupport.targetSchema(batch.schema(), options);
        if (options.isHudiSinkMode())
        {
            PixelsHudiMergeSupport.configureSessionForRecordLevelIndex(spark);
            targetSchema = PixelsHudiPartitioning.withPartitionColumnInSchema(targetSchema);
            List<String> partitionColumns = PixelsHudiMergeSupport.resolvePartitionColumns(primaryKeys, targetSchema);
            mergeCondition = PixelsMergeSupport.buildMergeCondition(primaryKeys, partitionColumns, false, false);
            PixelsHudiMergeSupport.ensureTargetTable(spark, targetSchema, primaryKeys, partitionColumns, options);
            PixelsHudiMergeSupport.validateTargetSchema(spark, targetSchema, options);
        }
        else
        {
            mergeCondition = PixelsMergeSupport.buildMergeCondition(primaryKeys, Collections.<String>emptyList(), true);
            PixelsDeltaMergeSupport.ensureTargetTable(spark, targetSchema, options);
            PixelsDeltaMergeSupport.ensureTargetTableProperties(spark, options);
            PixelsDeltaMergeSupport.ensureTargetColumns(spark, targetSchema, options);
            PixelsDeltaMergeSupport.validateTargetSchema(spark, targetSchema, options);
        }

        PixelsDeltaMergeContext context = new PixelsDeltaMergeContext(
                spark,
                options,
                targetSchema,
                primaryKeys,
                mergeCondition);

        PixelsDeltaPreparedBatch preparedBatch = PixelsDeltaPreparedBatchFactory.prepare(batch, primaryKeys, options);
        try
        {
            createSinkHandler(options).handle(preparedBatch, context, batchId);
        }
        finally
        {
            preparedBatch.close();
        }
    }

    private static boolean hasBucketColumn(StructType schema)
    {
        for (StructField field : schema.fields())
        {
            if (PixelsDeltaMergeColumns.BUCKET_ID.equals(field.name()))
            {
                return true;
            }
        }
        return false;
    }

    private static final class MergeForeachBatch implements VoidFunction2<Dataset<Row>, Long>
    {
        private final PixelsDeltaMergeOptions options;

        private MergeForeachBatch(PixelsDeltaMergeOptions options)
        {
            this.options = options;
        }

        @Override
        public void call(Dataset<Row> batch, Long batchId)
        {
            processBatch(batch, batchId, options);
        }
    }

    private static PixelsMergeSinkHandler createSinkHandler(PixelsDeltaMergeOptions options)
    {
        if (options.isNoopSinkMode())
        {
            return new PixelsNoopSinkHandler();
        }
        if (options.isHudiSinkMode())
        {
            return new PixelsHudiSinkHandler();
        }
        return new PixelsDeltaSinkHandler();
    }
}
