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

    private final String host;
    private final int port;
    private final String database;
    private final String table;
    private final List<Integer> buckets;
    private final String metadataHost;
    private final Integer metadataPort;

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
        this.buckets = parseBuckets(options.get(BUCKETS));
        this.metadataHost = firstNonEmpty(options.get(METADATA_HOST), config.get(PixelsSparkConfig.METADATA_HOST));
        String metadataPortValue = firstNonEmpty(options.get(METADATA_PORT), config.get(PixelsSparkConfig.METADATA_PORT));
        this.metadataPort = metadataPortValue != null ? Integer.parseInt(metadataPortValue) : null;
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

    private static List<Integer> parseBuckets(String buckets)
    {
        if (buckets == null || buckets.trim().isEmpty())
        {
            return Collections.emptyList();
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
