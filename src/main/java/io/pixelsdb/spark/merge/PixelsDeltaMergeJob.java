package io.pixelsdb.spark.merge;

import io.delta.tables.DeltaTable;
import io.pixelsdb.spark.metadata.PixelsTableMetadata;
import io.pixelsdb.spark.metadata.PixelsTableMetadataRegistry;
import io.pixelsdb.spark.source.PixelsMetadataColumns;
import io.pixelsdb.spark.source.PixelsSourceOptions;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

public final class PixelsDeltaMergeJob
{
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
                put(PixelsSourceOptions.METADATA_HOST, options.getMetadataHost());
                put(PixelsSourceOptions.METADATA_PORT, String.valueOf(options.getMetadataPort()));
            }});

            PixelsTableMetadata tableMetadata = PixelsTableMetadataRegistry.get(sourceOptions)
                    .getTableMetadata(options.getDatabase(), options.getTable());
            if (!tableMetadata.hasPrimaryKey())
            {
                throw new IllegalStateException("Table " + options.getDatabase() + "." + options.getTable()
                        + " does not have a primary key in Pixels metadata");
            }

            SparkSession spark = batch.sparkSession();
            StructType targetSchema = targetSchema(batch.schema(), options);
            ensureTargetTable(spark, targetSchema, options);
            validateTargetSchema(spark, targetSchema, options);

            List<String> primaryKeys = tableMetadata.getPrimaryKeyColumns();
            Dataset<Row> latest = latestPerPrimaryKey(batch, primaryKeys).cache();

            Dataset<Row> deletes = businessColumnsOnly(latest.filter(col(PixelsMetadataColumns.OP).equalTo("DELETE")));
            Dataset<Row> upserts = businessColumnsOnly(latest.filter(col(PixelsMetadataColumns.OP).notEqual("DELETE")));

            if (!upserts.isEmpty())
            {
                DeltaTable.forPath(spark, options.getTargetPath())
                        .as("t")
                        .merge(upserts.as("s"), buildMergeCondition(primaryKeys))
                        .whenMatched()
                        .updateExpr(buildUpsertAssignmentMap(targetSchema, options))
                        .whenNotMatched()
                        .insertExpr(buildUpsertAssignmentMap(targetSchema, options))
                        .execute();
            }

            if (!deletes.isEmpty() && isHardDelete(options))
            {
                DeltaTable.forPath(spark, options.getTargetPath())
                        .as("t")
                        .merge(deletes.as("s"), buildMergeCondition(primaryKeys))
                        .whenMatched()
                        .delete()
                        .execute();
            }
            else if (!deletes.isEmpty() && isSoftDelete(options))
            {
                DeltaTable.forPath(spark, options.getTargetPath())
                        .as("t")
                        .merge(deletes.as("s"), buildMergeCondition(primaryKeys))
                        .whenMatched()
                        .updateExpr(buildSoftDeleteAssignmentMap(targetSchema, options))
                        .execute();
            }

            latest.unpersist();
        }
    }

    private static Dataset<Row> latestPerPrimaryKey(Dataset<Row> batch, List<String> primaryKeys)
    {
        List<Column> partitions = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys)
        {
            partitions.add(col(primaryKey));
        }

        WindowSpec window = Window.partitionBy(partitions.toArray(new Column[0]))
                .orderBy(
                        col(PixelsMetadataColumns.TOTAL_ORDER).desc_nulls_last(),
                        col(PixelsMetadataColumns.DATA_COLLECTION_ORDER).desc_nulls_last(),
                        col(PixelsMetadataColumns.TXN_ID).desc_nulls_last());

        return batch.withColumn("_pixels_rank", row_number().over(window))
                .filter(col("_pixels_rank").equalTo(1))
                .drop("_pixels_rank");
    }

    private static Dataset<Row> businessColumnsOnly(Dataset<Row> batch)
    {
        List<String> columns = new ArrayList<>();
        for (StructField field : batch.schema().fields())
        {
            if (!PixelsMetadataColumns.isMetadataColumn(field.name()))
            {
                columns.add(field.name());
            }
        }
        return batch.selectExpr(columns.toArray(new String[0]));
    }

    private static StructType targetSchema(StructType schema, PixelsDeltaMergeOptions options)
    {
        List<StructField> fields = new ArrayList<>();
        for (StructField field : schema.fields())
        {
            if (!PixelsMetadataColumns.isMetadataColumn(field.name()))
            {
                fields.add(field);
            }
        }
        if (isSoftDelete(options))
        {
            fields.add(DataTypes.createStructField(PixelsDeltaMergeColumns.IS_DELETED, DataTypes.BooleanType, false));
            fields.add(DataTypes.createStructField(PixelsDeltaMergeColumns.DELETED_AT, DataTypes.TimestampType, true));
        }
        return new StructType(fields.toArray(new StructField[0]));
    }

    private static Map<String, String> buildUpsertAssignmentMap(StructType targetSchema, PixelsDeltaMergeOptions options)
    {
        Map<String, String> assignments = new LinkedHashMap<>();
        for (StructField field : targetSchema.fields())
        {
            if (PixelsDeltaMergeColumns.IS_DELETED.equals(field.name()))
            {
                assignments.put(field.name(), "false");
            }
            else if (PixelsDeltaMergeColumns.DELETED_AT.equals(field.name()))
            {
                assignments.put(field.name(), "CAST(NULL AS TIMESTAMP)");
            }
            else
            {
                assignments.put(field.name(), "s." + field.name());
            }
        }
        return assignments;
    }

    private static Map<String, String> buildSoftDeleteAssignmentMap(StructType targetSchema, PixelsDeltaMergeOptions options)
    {
        Map<String, String> assignments = new LinkedHashMap<>();
        for (StructField field : targetSchema.fields())
        {
            if (PixelsDeltaMergeColumns.IS_DELETED.equals(field.name()))
            {
                assignments.put(field.name(), "true");
            }
            else if (PixelsDeltaMergeColumns.DELETED_AT.equals(field.name()))
            {
                assignments.put(field.name(), "current_timestamp()");
            }
            else
            {
                assignments.put(field.name(), "t." + field.name());
            }
        }
        return assignments;
    }

    private static String buildMergeCondition(List<String> primaryKeys)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < primaryKeys.size(); i++)
        {
            if (i > 0)
            {
                builder.append(" AND ");
            }
            String primaryKey = primaryKeys.get(i);
            builder.append("t.").append(primaryKey).append(" <=> s.").append(primaryKey);
        }
        return builder.toString();
    }

    private static void ensureTargetTable(SparkSession spark, StructType targetSchema, PixelsDeltaMergeOptions options)
    {
        if (DeltaTable.isDeltaTable(spark, options.getTargetPath()))
        {
            return;
        }
        if (!options.isAutoCreateTable())
        {
            throw new IllegalStateException("Delta target path does not exist: " + options.getTargetPath());
        }

        spark.createDataFrame(Collections.<Row>emptyList(), targetSchema)
                .write()
                .format("delta")
                .mode("overwrite")
                .save(options.getTargetPath());
    }

    private static void validateTargetSchema(SparkSession spark, StructType expectedSchema, PixelsDeltaMergeOptions options)
    {
        StructType actualSchema = spark.read().format("delta").load(options.getTargetPath()).schema();
        for (StructField expectedField : expectedSchema.fields())
        {
            StructField actualField = findField(actualSchema, expectedField.name());
            if (actualField == null)
            {
                throw new IllegalStateException("Delta target is missing required column: " + expectedField.name());
            }
            if (!actualField.dataType().sameType(expectedField.dataType()))
            {
                throw new IllegalStateException("Delta target column type mismatch for " + expectedField.name()
                        + ", expected " + expectedField.dataType().catalogString()
                        + ", actual " + actualField.dataType().catalogString());
            }
        }
    }

    private static StructField findField(StructType schema, String fieldName)
    {
        for (StructField field : schema.fields())
        {
            if (field.name().equalsIgnoreCase(fieldName))
            {
                return field;
            }
        }
        return null;
    }

    private static boolean isHardDelete(PixelsDeltaMergeOptions options)
    {
        return !"ignore".equalsIgnoreCase(options.getDeleteMode())
                && !"soft".equalsIgnoreCase(options.getDeleteMode());
    }

    private static boolean isSoftDelete(PixelsDeltaMergeOptions options)
    {
        return "soft".equalsIgnoreCase(options.getDeleteMode());
    }
}
