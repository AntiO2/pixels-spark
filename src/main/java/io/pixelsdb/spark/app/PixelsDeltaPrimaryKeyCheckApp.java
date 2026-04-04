package io.pixelsdb.spark.app;

import io.pixelsdb.spark.config.PixelsSparkConfig;
import io.pixelsdb.spark.metadata.PixelsTableMetadata;
import io.pixelsdb.spark.metadata.PixelsTableMetadataRegistry;
import io.pixelsdb.spark.source.PixelsSourceOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PixelsDeltaPrimaryKeyCheckApp
{
    public static void main(String[] args)
    {
        if (args.length < 5)
        {
            throw new IllegalArgumentException("Usage: PixelsDeltaPrimaryKeyCheckApp "
                    + "<metadata-host> <metadata-port> <database> <table> <delta-path> [spark-master]");
        }

        String metadataHost = args[0];
        String metadataPort = args[1];
        String database = args[2];
        String table = args[3];
        String deltaPath = args[4];
        String sparkMaster = args.length > 5 ? args[5] : PixelsSparkConfig.instance().get(PixelsSparkConfig.SPARK_MASTER);

        PixelsSourceOptions sourceOptions = new PixelsSourceOptions(new LinkedHashMap<String, String>()
        {{
            put(PixelsSourceOptions.HOST, "localhost");
            put(PixelsSourceOptions.DATABASE, database);
            put(PixelsSourceOptions.TABLE, table);
            put(PixelsSourceOptions.METADATA_HOST, metadataHost);
            put(PixelsSourceOptions.METADATA_PORT, metadataPort);
        }});
        PixelsTableMetadata metadata = PixelsTableMetadataRegistry.get(sourceOptions)
                .getTableMetadata(database, table);
        List<String> primaryKeys = metadata.getPrimaryKeyColumns();
        if (primaryKeys.isEmpty())
        {
            throw new IllegalStateException("No primary key found for " + database + "." + table);
        }

        SparkSession.Builder builder = SparkSession.builder()
                .appName("pixels-delta-pk-check")
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
            long rowCount = dataset.count();
            long distinctPkCount = dataset.selectExpr(primaryKeys.toArray(new String[0])).distinct().count();

            System.out.println("database=" + database);
            System.out.println("table=" + table);
            System.out.println("delta_path=" + deltaPath);
            System.out.println("primary_keys=" + primaryKeys);
            System.out.println("row_count=" + rowCount);
            System.out.println("distinct_pk_count=" + distinctPkCount);
        }
        finally
        {
            spark.stop();
        }
    }
}
