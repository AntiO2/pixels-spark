package io.pixelsdb.spark.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class PixelsSavingAccountStreamTest
{
    public static void main(String[] args) throws Exception
    {
        String rpcHost = args.length > 0 ? args[0] : "localhost";
        String rpcPort = args.length > 1 ? args[1] : "9091";
        String metadataHost = args.length > 2 ? args[2] : "localhost";
        String metadataPort = args.length > 3 ? args[3] : "18888";
        String database = args.length > 4 ? args[4] : "pixels_bench";
        String table = args.length > 5 ? args[5] : "savingaccount";
        String buckets = args.length > 6 ? args[6] : "0";

        SparkSession spark = SparkSession.builder()
                .appName("pixels-savingaccount-stream-test")
                .master("local[1]")
                .getOrCreate();

        try
        {
            Dataset<Row> stream = spark.readStream()
                    .format("pixels")
                    .option("pixels.host", rpcHost)
                    .option("pixels.port", rpcPort)
                    .option("pixels.database", database)
                    .option("pixels.table", table)
                    .option("pixels.buckets", buckets)
                    .option("metadata.host", metadataHost)
                    .option("metadata.port", metadataPort)
                    .load();

            stream.printSchema();

            StreamingQuery query = stream.writeStream()
                    .format("console")
                    .outputMode("append")
                    .option("truncate", "false")
                    .trigger(Trigger.Once())
                    .start();

            query.awaitTermination();
        }
        finally
        {
            spark.stop();
        }
    }
}
