package io.pixelsdb.spark.merge;

import io.pixelsdb.spark.benchmark.BenchmarkTableDefinition;
import io.pixelsdb.spark.benchmark.BenchmarkTableRegistry;
import io.pixelsdb.spark.source.PixelsSourceOptions;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

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
        new MergeForeachBatch(options).call(batch, batchId);
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
            if (options.isNoopSinkMode())
            {
                LOG.info("sink-mode=noop batchId={} table={}.{} partitions={}",
                        batchId, options.getDatabase(), options.getTable(), batch.rdd().getNumPartitions());
                return;
            }

            StructType targetSchema = PixelsDeltaMergeSupport.targetSchema(batch.schema(), options);
            PixelsDeltaMergeSupport.ensureTargetTable(spark, targetSchema, options);
            PixelsDeltaMergeSupport.ensureTargetTableProperties(spark, options);
            PixelsDeltaMergeSupport.ensureTargetColumns(spark, targetSchema, options);
            PixelsDeltaMergeSupport.validateTargetSchema(spark, targetSchema, options);

            PixelsDeltaMergeContext context = new PixelsDeltaMergeContext(
                    spark,
                    options,
                    targetSchema,
                    primaryKeys,
                    PixelsDeltaMergeSupport.buildMergeCondition(primaryKeys));

            PixelsDeltaPreparedBatch preparedBatch = PixelsDeltaPreparedBatchFactory.prepare(batch, primaryKeys, options);
            try
            {
                applyHandlers(
                        Arrays.asList(
                                new PixelsDeltaInsertHandler(),
                                new PixelsDeltaUpdateHandler(),
                                new PixelsDeltaDeleteHandler()),
                        Arrays.asList(
                                preparedBatch.getInserts(),
                                preparedBatch.getUpdates(),
                                preparedBatch.getDeletes()),
                        context);
            }
            finally
            {
                preparedBatch.close();
            }
        }
    }

    private static void applyHandlers(
            List<PixelsDeltaOperationHandler> handlers,
            List<Dataset<Row>> operationBatches,
            PixelsDeltaMergeContext context)
    {
        for (int i = 0; i < handlers.size(); i++)
        {
            PixelsDeltaOperationHandler handler = handlers.get(i);
            Dataset<Row> rows = operationBatches.get(i);
            if (handler.canHandle(rows, context))
            {
                handler.handle(rows, context);
            }
        }
    }
}
