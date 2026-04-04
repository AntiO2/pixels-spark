package io.pixelsdb.spark.merge;

import io.pixelsdb.spark.config.PixelsSparkConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PixelsDeltaMergeOptions implements Serializable
{
    private final String rpcHost;
    private final int rpcPort;
    private final String metadataHost;
    private final int metadataPort;
    private final String database;
    private final String table;
    private final List<Integer> buckets;
    private final String targetPath;
    private final String checkpointLocation;
    private final String triggerMode;
    private final String triggerInterval;
    private final boolean autoCreateTable;
    private final String deleteMode;

    public PixelsDeltaMergeOptions(
            String rpcHost,
            int rpcPort,
            String metadataHost,
            int metadataPort,
            String database,
            String table,
            List<Integer> buckets,
            String targetPath,
            String checkpointLocation,
            String triggerMode,
            String triggerInterval,
            boolean autoCreateTable,
            String deleteMode)
    {
        this.rpcHost = rpcHost;
        this.rpcPort = rpcPort;
        this.metadataHost = metadataHost;
        this.metadataPort = metadataPort;
        this.database = database;
        this.table = table;
        this.buckets = buckets == null ? Collections.<Integer>emptyList() : new ArrayList<>(buckets);
        this.targetPath = targetPath;
        this.checkpointLocation = checkpointLocation;
        this.triggerMode = triggerMode;
        this.triggerInterval = triggerInterval;
        this.autoCreateTable = autoCreateTable;
        this.deleteMode = deleteMode;
    }

    public String getRpcHost()
    {
        return rpcHost;
    }

    public int getRpcPort()
    {
        return rpcPort;
    }

    public String getMetadataHost()
    {
        return metadataHost;
    }

    public int getMetadataPort()
    {
        return metadataPort;
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
        return Collections.unmodifiableList(buckets);
    }

    public String getTargetPath()
    {
        return targetPath;
    }

    public String getCheckpointLocation()
    {
        return checkpointLocation;
    }

    public String getTriggerInterval()
    {
        return triggerInterval;
    }

    public String getTriggerMode()
    {
        return triggerMode;
    }

    public boolean isAutoCreateTable()
    {
        return autoCreateTable;
    }

    public String getDeleteMode()
    {
        return deleteMode;
    }

    public static PixelsDeltaMergeOptions fromArguments(java.util.Map<String, String> args)
    {
        PixelsSparkConfig config = PixelsSparkConfig.instance();
        String database = require(args, "database");
        String table = require(args, "table");
        String targetPath = firstNonEmpty(args.get("target-path"), config.get(PixelsSparkConfig.DELTA_TARGET_PATH));
        String checkpointLocation = firstNonEmpty(
                args.get("checkpoint-location"),
                config.get(PixelsSparkConfig.DELTA_CHECKPOINT_LOCATION));

        if (targetPath == null)
        {
            throw new IllegalArgumentException("Missing required argument: --target-path");
        }
        if (checkpointLocation == null)
        {
            throw new IllegalArgumentException("Missing required argument: --checkpoint-location");
        }

        return new PixelsDeltaMergeOptions(
                firstNonEmpty(args.get("rpc-host"), config.getOrDefault(PixelsSparkConfig.RPC_HOST, "localhost")),
                Integer.parseInt(firstNonEmpty(args.get("rpc-port"),
                        String.valueOf(config.getIntOrDefault(PixelsSparkConfig.RPC_PORT, 9091)))),
                firstNonEmpty(args.get("metadata-host"),
                        config.getOrDefault(PixelsSparkConfig.METADATA_HOST, "localhost")),
                Integer.parseInt(firstNonEmpty(args.get("metadata-port"),
                        String.valueOf(config.getIntOrDefault(PixelsSparkConfig.METADATA_PORT, 18888)))),
                database,
                table,
                parseBuckets(args.get("buckets")),
                targetPath,
                checkpointLocation,
                firstNonEmpty(args.get("trigger-mode"),
                        config.getOrDefault(PixelsSparkConfig.DELTA_TRIGGER_MODE, "once")),
                firstNonEmpty(args.get("trigger-interval"),
                        config.getOrDefault(PixelsSparkConfig.DELTA_TRIGGER_INTERVAL, "0 seconds")),
                Boolean.parseBoolean(firstNonEmpty(args.get("auto-create-table"),
                        String.valueOf(config.getBooleanOrDefault(PixelsSparkConfig.DELTA_AUTO_CREATE, true)))),
                firstNonEmpty(args.get("delete-mode"),
                        config.getOrDefault(PixelsSparkConfig.DELTA_DELETE_MODE, "hard")));
    }

    private static String require(java.util.Map<String, String> args, String key)
    {
        String value = args.get(key);
        if (value == null || value.trim().isEmpty())
        {
            throw new IllegalArgumentException("Missing required argument: --" + key);
        }
        return value.trim();
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

    private static List<Integer> parseBuckets(String rawBuckets)
    {
        if (rawBuckets == null || rawBuckets.trim().isEmpty())
        {
            return Collections.emptyList();
        }
        List<Integer> result = new ArrayList<>();
        for (String bucket : rawBuckets.split(","))
        {
            String trimmed = bucket.trim();
            if (!trimmed.isEmpty())
            {
                result.add(Integer.parseInt(trimmed));
            }
        }
        return result;
    }
}
