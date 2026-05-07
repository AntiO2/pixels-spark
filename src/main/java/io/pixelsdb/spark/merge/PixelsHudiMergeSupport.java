package io.pixelsdb.spark.merge;

import io.pixelsdb.spark.config.PixelsSparkConfig;
import io.pixelsdb.spark.hudi.PixelsHudiPartitioning;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.UUID;

final class PixelsHudiMergeSupport
{
    private static final String HUDI_INDEX_RECORD_LEVEL = "RECORD_LEVEL_INDEX";

    private PixelsHudiMergeSupport()
    {
    }

    static void configureSessionForRecordLevelIndex(SparkSession spark)
    {
        spark.conf().set("hoodie.metadata.enable", "true");
        spark.conf().set("hoodie.metadata.record.level.index.enable", "true");
        // Disable global record index when using partitioned RLI.
        spark.conf().set("hoodie.metadata.record.index.enable", "false");
        spark.conf().set("hoodie.metadata.global.record.level.index.enable", "false");
        spark.conf().set("hoodie.index.type", HUDI_INDEX_RECORD_LEVEL);
    }

    static void ensureTargetTable(
            SparkSession spark,
            StructType targetSchema,
            List<String> primaryKeys,
            List<String> partitionColumns,
            PixelsDeltaMergeOptions options)
    {
        if (isHudiTable(spark, options.getTargetPath()))
        {
            ensureSparkCatalogTable(spark, options.getTargetPath());
            return;
        }
        if (!options.isAutoCreateTable())
        {
            throw new IllegalStateException("Hudi target path does not exist: " + options.getTargetPath());
        }

        spark.createDataFrame(Collections.<Row>emptyList(), targetSchema)
                .write()
                .format("hudi")
                .options(buildWriteOptions(targetSchema, primaryKeys, partitionColumns, options, "bulk_insert"))
                .mode("overwrite")
                .save(options.getTargetPath());
        ensureSparkCatalogTable(spark, options.getTargetPath());
    }

    static List<String> resolvePartitionColumns(List<String> primaryKeys, StructType schema)
    {
        for (String column : primaryKeys)
        {
            if (findField(schema, column) == null)
            {
                throw new IllegalArgumentException("Hudi partition key '" + column
                        + "' does not exist in table schema");
            }
        }
        if (findField(schema, PixelsHudiPartitioning.HUDI_PARTITION_COLUMN) == null)
        {
            throw new IllegalArgumentException("Hudi partition key '" + PixelsHudiPartitioning.HUDI_PARTITION_COLUMN
                    + "' does not exist in table schema");
        }
        return Arrays.asList(PixelsHudiPartitioning.HUDI_PARTITION_COLUMN);
    }

    static void validateTargetSchema(SparkSession spark, StructType expectedSchema, PixelsDeltaMergeOptions options)
    {
        StructType actualSchema = spark.read().format("hudi").load(options.getTargetPath()).schema();
        for (StructField expectedField : expectedSchema.fields())
        {
            StructField actualField = findField(actualSchema, expectedField.name());
            if (actualField == null)
            {
                throw new IllegalStateException("Hudi target is missing required column: " + expectedField.name());
            }
            if (!actualField.dataType().sameType(expectedField.dataType()))
            {
                throw new IllegalStateException("Hudi target column type mismatch for " + expectedField.name()
                        + ", expected " + expectedField.dataType().catalogString()
                        + ", actual " + actualField.dataType().catalogString());
            }
        }
    }

    static Dataset<Row> prepareRowsForMerge(
            Dataset<Row> rows,
            StructType targetSchema,
            List<String> primaryKeys)
    {
        int partitionCount = PixelsHudiPartitioning.resolvePartitionCount(
                PixelsSparkConfig.instance().getIntOrDefault("node.bucket.num", 0));
        Dataset<Row> partitioned = PixelsHudiPartitioning.withPartitionColumn(rows, primaryKeys, partitionCount);
        return PixelsDeltaPreparedBatchFactory.prepareInsertRows(partitioned, targetSchema);
    }

    static Map<String, String> buildUpsertAssignmentMap(StructType targetSchema)
    {
        Map<String, String> assignments = new LinkedHashMap<>();
        for (StructField field : targetSchema.fields())
        {
            if (PixelsDeltaMergeColumns.IS_DELETED.equals(field.name()))
            {
                assignments.put(quoteIdentifier(field.name()), "false");
            }
            else if (PixelsDeltaMergeColumns.DELETED_AT.equals(field.name()))
            {
                assignments.put(quoteIdentifier(field.name()), "CAST(NULL AS TIMESTAMP)");
            }
            else
            {
                assignments.put(quoteIdentifier(field.name()), "s." + quoteIdentifier(field.name()));
            }
        }
        return assignments;
    }

    static Map<String, String> buildSoftDeleteAssignmentMap(StructType targetSchema)
    {
        Map<String, String> assignments = new LinkedHashMap<>();
        for (StructField field : targetSchema.fields())
        {
            if (PixelsDeltaMergeColumns.IS_DELETED.equals(field.name()))
            {
                assignments.put(quoteIdentifier(field.name()), "true");
            }
            else if (PixelsDeltaMergeColumns.DELETED_AT.equals(field.name()))
            {
                assignments.put(quoteIdentifier(field.name()), "current_timestamp()");
            }
            else
            {
                assignments.put(quoteIdentifier(field.name()), "t." + quoteIdentifier(field.name()));
            }
        }
        return assignments;
    }

    static String targetTableIdentifier(String path)
    {
        return quoteIdentifier(tableNameForPath(path));
    }

    static String assignmentSql(Map<String, String> assignments)
    {
        StringBuilder builder = new StringBuilder();
        int index = 0;
        for (Map.Entry<String, String> entry : assignments.entrySet())
        {
            if (index++ > 0)
            {
                builder.append(", ");
            }
            builder.append(entry.getKey()).append(" = ").append(entry.getValue());
        }
        return builder.toString();
    }

    static String insertColumnsSql(StructType schema)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < schema.fields().length; i++)
        {
            if (i > 0)
            {
                builder.append(", ");
            }
            builder.append(quoteIdentifier(schema.fields()[i].name()));
        }
        return builder.toString();
    }

    static String insertValuesSql(StructType schema)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < schema.fields().length; i++)
        {
            if (i > 0)
            {
                builder.append(", ");
            }
            String column = schema.fields()[i].name();
            builder.append("s.").append(quoteIdentifier(column));
        }
        return builder.toString();
    }

    static String tempViewName(String prefix)
    {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static Map<String, String> buildWriteOptions(
            StructType targetSchema,
            List<String> primaryKeys,
            List<String> partitionColumns,
            PixelsDeltaMergeOptions options,
            String operation)
    {
        Map<String, String> writeOptions = new LinkedHashMap<>();
        writeOptions.put("hoodie.table.name", options.getTable().toLowerCase());
        writeOptions.put("hoodie.datasource.write.table.type", "COPY_ON_WRITE");
        writeOptions.put("hoodie.datasource.write.operation", operation);
        writeOptions.put("hoodie.datasource.write.recordkey.field", String.join(",", primaryKeys));
        writeOptions.put("hoodie.datasource.write.precombine.field", selectPrecombineField(targetSchema, primaryKeys));
        writeOptions.put("hoodie.metadata.enable", "true");
        writeOptions.put("hoodie.metadata.record.level.index.enable", "true");
        // Keep writer-side index strictly partitioned RLI.
        writeOptions.put("hoodie.metadata.record.index.enable", "false");
        writeOptions.put("hoodie.metadata.global.record.level.index.enable", "false");
        writeOptions.put("hoodie.index.type", HUDI_INDEX_RECORD_LEVEL);
        if (partitionColumns.isEmpty())
        {
            writeOptions.put("hoodie.datasource.write.partitionpath.field", "");
            writeOptions.put("hoodie.datasource.write.keygenerator.class",
                    "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
        }
        else
        {
            writeOptions.put("hoodie.datasource.write.partitionpath.field", String.join(",", partitionColumns));
            writeOptions.put("hoodie.datasource.write.keygenerator.class",
                    partitionColumns.size() == 1
                            ? "org.apache.hudi.keygen.SimpleKeyGenerator"
                            : "org.apache.hudi.keygen.ComplexKeyGenerator");
        }
        return writeOptions;
    }

    private static String selectPrecombineField(StructType schema, List<String> primaryKeys)
    {
        if (findField(schema, "freshness_ts") != null)
        {
            return "freshness_ts";
        }
        if (findField(schema, "last_update_timestamp") != null)
        {
            return "last_update_timestamp";
        }
        if (findField(schema, "ts") != null)
        {
            return "ts";
        }
        return primaryKeys.get(0);
    }


    private static boolean isHudiTable(SparkSession spark, String path)
    {
        try
        {
            spark.read().format("hudi").load(path).schema();
            return true;
        }
        catch (Exception e)
        {
            return false;
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

    private static String quoteIdentifier(String identifier)
    {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static void ensureSparkCatalogTable(SparkSession spark, String path)
    {
        String tableName = targetTableIdentifier(path);
        spark.sql("CREATE TABLE IF NOT EXISTS " + tableName
                + " USING hudi LOCATION '" + escapeSqlLiteral(path) + "'");
    }

    private static String tableNameForPath(String path)
    {
        UUID stableId = UUID.nameUUIDFromBytes(path.getBytes(StandardCharsets.UTF_8));
        return "pixels_hudi_target_" + stableId.toString().replace("-", "");
    }

    private static String escapeSqlLiteral(String value)
    {
        return value.replace("'", "''");
    }
}
