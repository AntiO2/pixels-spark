package io.pixelsdb.spark.app;

import io.pixelsdb.spark.benchmark.BenchmarkTableDefinition;
import io.pixelsdb.spark.benchmark.BenchmarkTableRegistry;
import io.pixelsdb.spark.config.PixelsSparkConfig;
import io.pixelsdb.spark.hudi.PixelsHudiPartitioning;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class PixelsBenchmarkHudiImportApp
{
    private static final String HUDI_INDEX_SIMPLE = "SIMPLE";
    private static final String HUDI_INDEX_RECORD_LEVEL = "RECORD_LEVEL_INDEX";

    public static void main(String[] args)
    {
        if (args.length < 4)
        {
            throw new IllegalArgumentException(
                    "Usage: PixelsBenchmarkHudiImportApp <csv-path> <target-path> <spark-master> <table-name> [benchmark]");
        }

        String csvPath = args[0];
        String targetPath = normalizeS3Path(args[1]);
        String sparkMaster = args[2];
        String tableName = args[3];
        String benchmark = args.length > 4 ? args[4] : BenchmarkTableRegistry.BENCHMARK_HYBENCH;

        BenchmarkTableDefinition definition = BenchmarkTableRegistry.require(benchmark, tableName);
        String recordKeyField = String.join(",", definition.getPrimaryKeyColumns());
        String precombineField = selectPrecombineField(definition);
        String hoodieTableName = definition.getTableName().toLowerCase(Locale.ROOT);
        int hudiPartitionCount = PixelsHudiPartitioning.resolvePartitionCount(
                PixelsSparkConfig.instance().getIntOrDefault("node.bucket.num", 0));
        String keyGeneratorClass = definition.getPrimaryKeyColumns().size() > 1
                ? "org.apache.hudi.keygen.ComplexKeyGenerator"
                : "org.apache.hudi.keygen.SimpleKeyGenerator";

        SparkSession.Builder builder = SparkSession.builder()
                .appName("pixels-benchmark-hudi-import-" + hoodieTableName)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog");

        if (sparkMaster != null && !sparkMaster.trim().isEmpty())
        {
            builder.master(sparkMaster.trim());
        }

        SparkSession spark = builder.getOrCreate();
        try
        {
            Dataset<Row> dataset = spark.read()
                    .format("csv")
                    .schema(definition.getSchema())
                    .option("header", "false")
                    .option("mode", "FAILFAST")
                    .option("nullValue", "")
                    .option("delimiter", definition.getDelimiter())
                    .load(csvPath);
            Dataset<Row> partitioned = PixelsHudiPartitioning.withPartitionColumn(
                    dataset,
                    definition.getPrimaryKeyColumns(),
                    hudiPartitionCount);

            Map<String, String> options = new LinkedHashMap<>();
            options.put("hoodie.table.name", hoodieTableName);
            options.put("hoodie.datasource.write.table.type", "COPY_ON_WRITE");
            options.put("hoodie.datasource.write.operation", "bulk_insert");
            options.put("hoodie.datasource.write.recordkey.field", recordKeyField);
            options.put("hoodie.datasource.write.precombine.field", precombineField);
            options.put("hoodie.datasource.write.partitionpath.field", PixelsHudiPartitioning.HUDI_PARTITION_COLUMN);
            options.put("hoodie.datasource.write.keygenerator.class", keyGeneratorClass);
            options.put("hoodie.insert.shuffle.parallelism", String.valueOf(Math.max(1, hudiPartitionCount)));
            options.put("hoodie.index.type", HUDI_INDEX_SIMPLE);
            options.put("hoodie.metadata.enable", "false");
            options.put("hoodie.metadata.record.index.enable", "false");

            partitioned.repartition(hudiPartitionCount, partitioned.col(PixelsHudiPartitioning.HUDI_PARTITION_COLUMN))
                    .write()
                    .format("hudi")
                    .options(options)
                    .mode("overwrite")
                    .save(targetPath);
            if (shouldBuildRecordLevelIndex(definition))
            {
                enableRecordLevelIndex(
                        spark,
                        partitioned.limit(1),
                        hoodieTableName,
                        recordKeyField,
                        precombineField,
                        keyGeneratorClass,
                        targetPath,
                        hudiPartitionCount);
            }

            long rowCount = dataset.count();
            System.out.println("table=" + hoodieTableName
                    + " csv_path=" + csvPath
                    + " target_path=" + targetPath
                    + " benchmark=" + benchmark
                    + " record_key_field=" + recordKeyField
                    + " precombine_field=" + precombineField
                    + " hudi_partition_count=" + hudiPartitionCount
                    + " index_type=" + HUDI_INDEX_RECORD_LEVEL
                    + " metadata_record_level_index=true (post-import)"
                    + " metadata_record_index=false (post-import)"
                    + " row_count=" + rowCount);
        }
        finally
        {
            spark.stop();
        }
    }

    private static String selectPrecombineField(BenchmarkTableDefinition definition)
    {
        if (hasField(definition, "freshness_ts"))
        {
            return "freshness_ts";
        }
        if (hasField(definition, "last_update_timestamp"))
        {
            return "last_update_timestamp";
        }
        if (hasField(definition, "ts"))
        {
            return "ts";
        }
        return definition.getSchema().fields()[0].name();
    }

    private static boolean hasField(BenchmarkTableDefinition definition, String fieldName)
    {
        for (StructField field : definition.getSchema().fields())
        {
            if (field.name().equalsIgnoreCase(fieldName))
            {
                return true;
            }
        }
        return false;
    }

    private static String normalizeS3Path(String path)
    {
        if (path == null)
        {
            return null;
        }
        if (path.startsWith("s3://"))
        {
            return "s3a://" + path.substring("s3://".length());
        }
        return path;
    }

    private static void enableRecordLevelIndex(
            SparkSession spark,
            Dataset<Row> sampleRows,
            String hoodieTableName,
            String recordKeyField,
            String precombineField,
            String keyGeneratorClass,
            String targetPath,
            int hudiPartitionCount)
    {
        if (sampleRows.isEmpty())
        {
            return;
        }

        sampleRows.write()
                .format("hudi")
                .mode("append")
                .option("hoodie.table.name", hoodieTableName)
                .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
                .option("hoodie.datasource.write.operation", "upsert")
                .option("hoodie.datasource.write.recordkey.field", recordKeyField)
                .option("hoodie.datasource.write.precombine.field", precombineField)
                .option("hoodie.datasource.write.partitionpath.field", PixelsHudiPartitioning.HUDI_PARTITION_COLUMN)
                .option("hoodie.datasource.write.keygenerator.class", keyGeneratorClass)
                .option("hoodie.insert.shuffle.parallelism", "1")
                .option("hoodie.upsert.shuffle.parallelism", String.valueOf(Math.max(1, hudiPartitionCount)))
                .option("hoodie.metadata.enable", "true")
                .option("hoodie.metadata.record.level.index.enable", "true")
                .option("hoodie.metadata.record.index.enable", "false")
                .option("hoodie.metadata.global.record.level.index.enable", "false")
                .option("hoodie.index.type", HUDI_INDEX_RECORD_LEVEL)
                .save(targetPath);
    }

    private static boolean shouldBuildRecordLevelIndex(BenchmarkTableDefinition definition)
    {
        return !"transfer".equalsIgnoreCase(definition.getTableName());
    }
}
