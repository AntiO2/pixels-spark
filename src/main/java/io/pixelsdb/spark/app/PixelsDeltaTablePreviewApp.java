package io.pixelsdb.spark.app;

import io.pixelsdb.spark.config.PixelsSparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PixelsDeltaTablePreviewApp
{
    public static void main(String[] args)
    {
        if (args.length < 1)
        {
            throw new IllegalArgumentException("Usage: PixelsDeltaTablePreviewApp <delta-path> [limit] [spark-master]");
        }

        String deltaPath = args[0];
        int limit = args.length > 1 ? Integer.parseInt(args[1]) : 20;
        String sparkMaster = args.length > 2 ? args[2] : PixelsSparkConfig.instance().get(PixelsSparkConfig.SPARK_MASTER);

        SparkSession.Builder builder = SparkSession.builder()
                .appName("pixels-delta-preview")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");

        if (sparkMaster != null && !sparkMaster.trim().isEmpty())
        {
            builder.master(sparkMaster.trim());
        }

        SparkSession spark = builder.getOrCreate();
        try
        {
            Dataset<Row> dataset = spark.read().format("delta").load(deltaPath);
            System.out.println("delta_path=" + deltaPath);
            dataset.printSchema();
            System.out.println("row_count=" + dataset.count());
            dataset.show(limit, false);
        }
        finally
        {
            spark.stop();
        }
    }
}
