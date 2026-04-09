package io.pixelsdb.spark.app;

import io.pixelsdb.spark.config.PixelsSparkConfig;
import io.pixelsdb.spark.merge.PixelsDeltaMergeJob;
import io.pixelsdb.spark.merge.PixelsDeltaMergeOptions;
import io.pixelsdb.spark.merge.PixelsDeltaMergePollingJob;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.LinkedHashMap;
import java.util.Map;

public class PixelsDeltaMergeApp
{
    public static void main(String[] args) throws Exception
    {
        Map<String, String> parsedArgs = parseArgs(args);
        PixelsDeltaMergeOptions options = PixelsDeltaMergeOptions.fromArguments(parsedArgs);

        SparkSession.Builder builder = SparkSession.builder()
                .appName("pixels-delta-merge")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");

        String sparkMaster = parsedArgs.get("spark-master");
        if (sparkMaster == null || sparkMaster.trim().isEmpty())
        {
            sparkMaster = PixelsSparkConfig.instance().get(PixelsSparkConfig.SPARK_MASTER);
        }
        if (sparkMaster != null && !sparkMaster.trim().isEmpty())
        {
            builder.master(sparkMaster.trim());
        }

        SparkSession spark = builder.getOrCreate();
        try
        {
            if (options.isPollingMode())
            {
                PixelsDeltaMergePollingJob.run(spark, options);
            }
            else
            {
                StreamingQuery query = PixelsDeltaMergeJob.start(spark, options);
                query.awaitTermination();
            }
        }
        finally
        {
            spark.stop();
        }
    }

    private static Map<String, String> parseArgs(String[] args)
    {
        Map<String, String> parsed = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++)
        {
            String key = args[i];
            if (!key.startsWith("--"))
            {
                throw new IllegalArgumentException("Expected --key value arguments, found: " + key);
            }
            if (i + 1 >= args.length)
            {
                throw new IllegalArgumentException("Missing value for argument: " + key);
            }
            parsed.put(key.substring(2), args[++i]);
        }
        return parsed;
    }
}
