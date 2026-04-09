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
    private final String mode;
    private final String triggerMode;
    private final String triggerInterval;
    private final String sinkMode;
    private final boolean autoCreateTable;
    private final String deleteMode;
    private final boolean enableDeletionVectors;

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
            String mode,
            String triggerMode,
            String triggerInterval,
            String sinkMode,
            boolean autoCreateTable,
            String deleteMode,
            boolean enableDeletionVectors)
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
        this.mode = normalizeMode(mode);
        this.triggerMode = triggerMode;
        this.triggerInterval = triggerInterval;
        this.sinkMode = normalizeSinkMode(sinkMode);
        this.autoCreateTable = autoCreateTable;
        this.deleteMode = deleteMode;
        this.enableDeletionVectors = enableDeletionVectors;
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

    public String getMode()
    {
        return mode;
    }

    public boolean isPollingMode()
    {
        return "polling".equalsIgnoreCase(mode);
    }

    public boolean isStreamingMode()
    {
        return "streaming".equalsIgnoreCase(mode);
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

    public String getSinkMode()
    {
        return sinkMode;
    }

    public boolean isNoopSinkMode()
    {
        return "noop".equalsIgnoreCase(sinkMode);
    }

    public String getDeleteMode()
    {
        return deleteMode;
    }

    public boolean isEnableDeletionVectors()
    {
        return enableDeletionVectors;
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
        String mode = firstNonEmpty(
                args.get("mode"),
                config.getOrDefault(PixelsSparkConfig.DELTA_MODE, "polling"));
        String sinkMode = firstNonEmpty(
                args.get("sink-mode"),
                config.getOrDefault(PixelsSparkConfig.DELTA_SINK_MODE, "delta"));

        if (!"noop".equalsIgnoreCase(sinkMode) && targetPath == null)
        {
            throw new IllegalArgumentException("Missing required argument: --target-path");
        }
        if ("streaming".equalsIgnoreCase(mode) && checkpointLocation == null)
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
                mode,
                firstNonEmpty(args.get("trigger-mode"),
                        config.getOrDefault(PixelsSparkConfig.DELTA_TRIGGER_MODE, "once")),
                firstNonEmpty(args.get("trigger-interval"),
                        config.getOrDefault(PixelsSparkConfig.DELTA_TRIGGER_INTERVAL, "0 seconds")),
                sinkMode,
                Boolean.parseBoolean(firstNonEmpty(args.get("auto-create-table"),
                        String.valueOf(config.getBooleanOrDefault(PixelsSparkConfig.DELTA_AUTO_CREATE, true)))),
                firstNonEmpty(args.get("delete-mode"),
                        config.getOrDefault(PixelsSparkConfig.DELTA_DELETE_MODE, "hard")),
                Boolean.parseBoolean(firstNonEmpty(args.get("enable-deletion-vectors"),
                        String.valueOf(config.getBooleanOrDefault(
                                PixelsSparkConfig.DELTA_ENABLE_DELETION_VECTORS, true)))));
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

    private static String normalizeSinkMode(String raw)
    {
        String value = firstNonEmpty(raw, "delta");
        if (!"delta".equalsIgnoreCase(value) && !"noop".equalsIgnoreCase(value))
        {
            throw new IllegalArgumentException("sink-mode must be one of: delta, noop");
        }
        return value.toLowerCase();
    }

    private static String normalizeMode(String raw)
    {
        String value = firstNonEmpty(raw, "polling");
        if (!"polling".equalsIgnoreCase(value) && !"streaming".equalsIgnoreCase(value))
        {
            throw new IllegalArgumentException("mode must be one of: polling, streaming");
        }
        return value.toLowerCase();
    }
}
