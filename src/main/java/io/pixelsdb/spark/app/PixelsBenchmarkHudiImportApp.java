package io.pixelsdb.spark.app;

import io.pixelsdb.spark.benchmark.BenchmarkTableDefinition;
import io.pixelsdb.spark.benchmark.BenchmarkTableRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class PixelsBenchmarkHudiImportApp
{
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
        String recordKeyField = definition.getSchema().fields()[0].name();
        String precombineField = selectPrecombineField(definition);
        String hoodieTableName = definition.getTableName().toLowerCase(Locale.ROOT);

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

            Map<String, String> options = new LinkedHashMap<>();
            options.put("hoodie.table.name", hoodieTableName);
            options.put("hoodie.datasource.write.table.type", "COPY_ON_WRITE");
            options.put("hoodie.datasource.write.operation", "bulk_insert");
            options.put("hoodie.datasource.write.recordkey.field", recordKeyField);
            options.put("hoodie.datasource.write.precombine.field", precombineField);
            options.put("hoodie.datasource.write.partitionpath.field", "");
            options.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
            options.put("hoodie.index.type", "RECORD_INDEX");
            options.put("hoodie.metadata.enable", "true");
            options.put("hoodie.metadata.record.index.enable", "true");

            dataset.write()
                    .format("hudi")
                    .options(options)
                    .mode("overwrite")
                    .save(targetPath);

            long rowCount = dataset.count();
            System.out.println("table=" + hoodieTableName
                    + " csv_path=" + csvPath
                    + " target_path=" + targetPath
                    + " benchmark=" + benchmark
                    + " record_key_field=" + recordKeyField
                    + " precombine_field=" + precombineField
                    + " index_type=RECORD_INDEX"
                    + " metadata_record_index=true"
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
}
