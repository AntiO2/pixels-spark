package io.pixelsdb.spark.merge;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class PixelsDeltaMergeSupport
{
    static final String DELTA_ENABLE_DELETION_VECTORS_PROPERTY = "delta.enableDeletionVectors";

    private PixelsDeltaMergeSupport()
    {
    }

    static StructType targetSchema(StructType schema, PixelsDeltaMergeOptions options)
    {
        return PixelsMergeSupport.targetSchema(schema, options);
    }

    static String buildMergeCondition(List<String> primaryKeys)
    {
        return PixelsMergeSupport.buildMergeCondition(primaryKeys, Collections.<String>emptyList(), true);
    }

    static Map<String, String> buildUpsertAssignmentMap(StructType targetSchema, PixelsDeltaMergeOptions options)
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

    static Map<String, String> buildSoftDeleteAssignmentMap(StructType targetSchema, PixelsDeltaMergeOptions options)
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

    static void ensureTargetTable(SparkSession spark, StructType targetSchema, PixelsDeltaMergeOptions options)
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
                .option(DELTA_ENABLE_DELETION_VECTORS_PROPERTY, String.valueOf(options.isEnableDeletionVectors()))
                .partitionBy(partitionColumns())
                .save(options.getTargetPath());
    }

    static void ensureTargetTableProperties(SparkSession spark, PixelsDeltaMergeOptions options)
    {
        if (!DeltaTable.isDeltaTable(spark, options.getTargetPath()))
        {
            return;
        }

        spark.sql("ALTER TABLE delta.`" + options.getTargetPath() + "` SET TBLPROPERTIES ("
                + "'" + DELTA_ENABLE_DELETION_VECTORS_PROPERTY + "'='"
                + String.valueOf(options.isEnableDeletionVectors()) + "')");
    }

    static void ensureTargetColumns(SparkSession spark, StructType expectedSchema, PixelsDeltaMergeOptions options)
    {
        if (!DeltaTable.isDeltaTable(spark, options.getTargetPath()))
        {
            return;
        }

        StructType actualSchema = spark.read().format("delta").load(options.getTargetPath()).schema();
        for (StructField expectedField : expectedSchema.fields())
        {
            if (findField(actualSchema, expectedField.name()) == null)
            {
                spark.sql("ALTER TABLE delta.`" + options.getTargetPath() + "` ADD COLUMNS ("
                        + expectedField.name() + " " + expectedField.dataType().catalogString() + ")");
            }
        }
    }

    static void validateTargetSchema(SparkSession spark, StructType expectedSchema, PixelsDeltaMergeOptions options)
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

    static boolean isHardDelete(PixelsDeltaMergeOptions options)
    {
        return PixelsMergeSupport.isHardDelete(options);
    }

    static boolean isSoftDelete(PixelsDeltaMergeOptions options)
    {
        return PixelsMergeSupport.isSoftDelete(options);
    }

    private static String[] partitionColumns()
    {
        return new String[] {PixelsDeltaMergeColumns.BUCKET_ID};
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
}
