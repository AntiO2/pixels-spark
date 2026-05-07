package io.pixelsdb.spark.merge;

import io.pixelsdb.spark.config.PixelsSparkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class PixelsDeltaMergeOptions implements Serializable
{
    private static final Logger LOG = LoggerFactory.getLogger(PixelsDeltaMergeOptions.class);

    private final String rpcHost;
    private final int rpcPort;
    private final String metadataHost;
    private final int metadataPort;
    private final String database;
    private final String table;
    private final String benchmark;
    private final List<Integer> buckets;
    private final String targetPath;
    private final String checkpointLocation;
    private final String mode;
    private final String triggerMode;
    private final String triggerInterval;
    private final String sinkMode;
    private final Set<Integer> noopBuckets;
    private final boolean enableLatestPerPrimaryKey;
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
            String benchmark,
            List<Integer> buckets,
            String targetPath,
            String checkpointLocation,
            String mode,
            String triggerMode,
            String triggerInterval,
            String sinkMode,
            Set<Integer> noopBuckets,
            boolean enableLatestPerPrimaryKey,
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
        this.benchmark = benchmark;
        this.buckets = buckets == null ? Collections.<Integer>emptyList() : new ArrayList<>(buckets);
        this.targetPath = targetPath;
        this.checkpointLocation = checkpointLocation;
        this.mode = normalizeMode(mode);
        this.triggerMode = triggerMode;
        this.triggerInterval = triggerInterval;
        this.sinkMode = normalizeSinkMode(sinkMode);
        this.noopBuckets = noopBuckets == null ? Collections.<Integer>emptySet() : new LinkedHashSet<>(noopBuckets);
        this.enableLatestPerPrimaryKey = enableLatestPerPrimaryKey;
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

    public String getBenchmark()
    {
        return benchmark;
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

    public boolean isHudiSinkMode()
    {
        return "hudi".equalsIgnoreCase(sinkMode);
    }

    public boolean isDeltaSinkMode()
    {
        return "delta".equalsIgnoreCase(sinkMode);
    }

    public Set<Integer> getNoopBuckets()
    {
        return Collections.unmodifiableSet(noopBuckets);
    }

    public boolean isBucketNoop(int bucketId)
    {
        return noopBuckets.contains(bucketId);
    }

    public boolean hasBucketSpecificSinkMode()
    {
        return !noopBuckets.isEmpty();
    }

    public String sinkModeForBucket(int bucketId)
    {
        return isBucketNoop(bucketId) ? "noop" : sinkMode;
    }

    public PixelsDeltaMergeOptions withSinkMode(String updatedSinkMode)
    {
        return new PixelsDeltaMergeOptions(
                rpcHost,
                rpcPort,
                metadataHost,
                metadataPort,
                database,
                table,
                benchmark,
                buckets,
                targetPath,
                checkpointLocation,
                mode,
                triggerMode,
                triggerInterval,
                updatedSinkMode,
                noopBuckets,
                enableLatestPerPrimaryKey,
                autoCreateTable,
                deleteMode,
                enableDeletionVectors);
    }

    public boolean isEnableLatestPerPrimaryKey()
    {
        return enableLatestPerPrimaryKey;
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
        String cdcExecutionMode = config.get(PixelsSparkConfig.CDC_EXECUTION_MODE);
        String legacyDeltaMode = config.get(PixelsSparkConfig.DELTA_MODE);
        if (args.get("mode") == null && cdcExecutionMode == null && legacyDeltaMode != null)
        {
            LOG.warn("Config '{}' is deprecated; use '{}' instead.",
                    PixelsSparkConfig.DELTA_MODE,
                    PixelsSparkConfig.CDC_EXECUTION_MODE);
        }
        String mode = firstNonEmpty(
                args.get("mode"),
                firstNonEmpty(cdcExecutionMode, firstNonEmpty(legacyDeltaMode, "polling")));
        String sinkMode = firstNonEmpty(
                args.get("sink-mode"),
                config.getOrDefault(PixelsSparkConfig.SINK_MODE, "delta"));
        Set<Integer> noopBuckets = parseBucketRanges(firstNonEmpty(
                args.get("noop-buckets"),
                config.get(PixelsSparkConfig.DELTA_NOOP_BUCKETS)));

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
                firstNonEmpty(args.get("benchmark"),
                        config.getOrDefault(PixelsSparkConfig.CDC_BENCHMARK, "hybench")),
                parseBuckets(args.get("buckets")),
                targetPath,
                checkpointLocation,
                mode,
                firstNonEmpty(args.get("trigger-mode"),
                        config.getOrDefault(PixelsSparkConfig.DELTA_TRIGGER_MODE, "once")),
                firstNonEmpty(args.get("trigger-interval"),
                        config.getOrDefault(PixelsSparkConfig.DELTA_TRIGGER_INTERVAL, "0 seconds")),
                sinkMode,
                noopBuckets,
                Boolean.parseBoolean(firstNonEmpty(args.get("enable-latest-per-primary-key"),
                        String.valueOf(config.getBooleanOrDefault(
                                PixelsSparkConfig.DELTA_ENABLE_LATEST_PER_PRIMARY_KEY, true)))),
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
        if (!"delta".equalsIgnoreCase(value)
                && !"hudi".equalsIgnoreCase(value)
                && !"noop".equalsIgnoreCase(value))
        {
            throw new IllegalArgumentException("sink-mode must be one of: delta, hudi, noop");
        }
        return value.toLowerCase();
    }

    private static Set<Integer> parseBucketRanges(String raw)
    {
        if (raw == null || raw.trim().isEmpty())
        {
            return Collections.emptySet();
        }

        Set<Integer> result = new LinkedHashSet<>();
        for (String token : raw.split(","))
        {
            String trimmed = token.trim();
            if (trimmed.isEmpty())
            {
                continue;
            }

            int dash = trimmed.indexOf('-');
            if (dash < 0)
            {
                result.add(Integer.parseInt(trimmed));
                continue;
            }

            if (dash == 0 || dash == trimmed.length() - 1 || trimmed.indexOf('-', dash + 1) >= 0)
            {
                throw new IllegalArgumentException("Invalid bucket range: " + trimmed);
            }

            int start = Integer.parseInt(trimmed.substring(0, dash).trim());
            int end = Integer.parseInt(trimmed.substring(dash + 1).trim());
            if (end < start)
            {
                throw new IllegalArgumentException("Invalid bucket range: " + trimmed);
            }
            for (int bucket = start; bucket <= end; bucket++)
            {
                result.add(bucket);
            }
        }
        return result;
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
