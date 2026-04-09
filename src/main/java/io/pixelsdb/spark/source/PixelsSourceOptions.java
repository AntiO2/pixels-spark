package io.pixelsdb.spark.source;

import io.pixelsdb.spark.config.PixelsSparkConfig;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PixelsSourceOptions implements Serializable
{
    public static final String HOST = "pixels.host";
    public static final String PORT = "pixels.port";
    public static final String DATABASE = "pixels.database";
    public static final String TABLE = "pixels.table";
    public static final String BUCKETS = "pixels.buckets";
    public static final String METADATA_HOST = "metadata.host";
    public static final String METADATA_PORT = "metadata.port";
    public static final String MAX_ROWS_PER_BATCH = "pixels.maxRowsPerBatch";
    public static final String MAX_WAIT_MS_PER_BATCH = "pixels.maxWaitMsPerBatch";
    public static final String EMPTY_POLL_SLEEP_MS = "pixels.emptyPollSleepMs";

    private final String host;
    private final int port;
    private final String database;
    private final String table;
    private final List<Integer> buckets;
    private final String metadataHost;
    private final Integer metadataPort;
    private final int maxRowsPerBatch;
    private final long maxWaitMsPerBatch;
    private final long emptyPollSleepMs;

    public PixelsSourceOptions(CaseInsensitiveStringMap options)
    {
        this(options.asCaseSensitiveMap());
    }

    public PixelsSourceOptions(Map<String, String> options)
    {
        PixelsSparkConfig config = PixelsSparkConfig.instance();
        this.host = firstNonEmpty(options.get(HOST), config.getOrDefault(PixelsSparkConfig.RPC_HOST, "localhost"));
        this.port = Integer.parseInt(firstNonEmpty(options.get(PORT),
                String.valueOf(config.getIntOrDefault(PixelsSparkConfig.RPC_PORT, 9091))));
        this.database = require(options, DATABASE);
        this.table = require(options, TABLE);
        this.buckets = parseBuckets(options.get(BUCKETS), config);
        this.metadataHost = firstNonEmpty(options.get(METADATA_HOST), config.get(PixelsSparkConfig.METADATA_HOST));
        String metadataPortValue = firstNonEmpty(options.get(METADATA_PORT), config.get(PixelsSparkConfig.METADATA_PORT));
        this.metadataPort = metadataPortValue != null ? Integer.parseInt(metadataPortValue) : null;
        this.maxRowsPerBatch = Math.max(1, Integer.parseInt(firstNonEmpty(options.get(MAX_ROWS_PER_BATCH),
                String.valueOf(config.getIntOrDefault(PixelsSparkConfig.SOURCE_MAX_ROWS_PER_BATCH, 100000)))));
        this.maxWaitMsPerBatch = Math.max(0L, Long.parseLong(firstNonEmpty(options.get(MAX_WAIT_MS_PER_BATCH),
                String.valueOf(config.getIntOrDefault(PixelsSparkConfig.SOURCE_MAX_WAIT_MS_PER_BATCH, 1000)))));
        this.emptyPollSleepMs = Math.max(0L, Long.parseLong(firstNonEmpty(options.get(EMPTY_POLL_SLEEP_MS),
                String.valueOf(config.getIntOrDefault(PixelsSparkConfig.SOURCE_EMPTY_POLL_SLEEP_MS, 100)))));
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    public String getDatabase()
    {
        return database;
    }

    public String getTable()
    {
        return table;
    }

    public List<Integer> getBuckets()
    {
        return buckets;
    }

    public String getMetadataHost()
    {
        return metadataHost;
    }

    public Integer getMetadataPort()
    {
        return metadataPort;
    }

    public int getMaxRowsPerBatch()
    {
        return maxRowsPerBatch;
    }

    public long getMaxWaitMsPerBatch()
    {
        return maxWaitMsPerBatch;
    }

    public long getEmptyPollSleepMs()
    {
        return emptyPollSleepMs;
    }

    private static String require(Map<String, String> options, String key)
    {
        String value = options.get(key);
        if (value == null || value.trim().isEmpty())
        {
            throw new IllegalArgumentException("Missing required option: " + key);
        }
        return value;
    }

    private static String firstNonEmpty(String primary, String fallback)
    {
        if (primary != null && !primary.trim().isEmpty())
        {
            return primary.trim();
        }
        if (fallback != null && !fallback.trim().isEmpty())
        {
            return fallback.trim();
        }
        return null;
    }

    private static List<Integer> parseBuckets(String buckets, PixelsSparkConfig config)
    {
        if (buckets == null || buckets.trim().isEmpty())
        {
            int bucketNum = config.getIntOrDefault("node.bucket.num", 0);
            if (bucketNum <= 0)
            {
                return Collections.emptyList();
            }

            List<Integer> allBuckets = new ArrayList<>(bucketNum);
            for (int bucketId = 0; bucketId < bucketNum; bucketId++)
            {
                allBuckets.add(bucketId);
            }
            return allBuckets;
        }

        List<Integer> result = new ArrayList<>();
        for (String part : buckets.split(","))
        {
            String trimmed = part.trim();
            if (!trimmed.isEmpty())
            {
                result.add(Integer.parseInt(trimmed));
            }
        }
        return result;
    }
}
