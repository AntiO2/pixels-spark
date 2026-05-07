package io.pixelsdb.spark.config;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class PixelsSparkConfig
{
    public static final String PIXELS_SPARK_CONFIG_ENV = "PIXELS_SPARK_CONFIG";
    public static final String DEFAULT_PIXELS_SPARK_CONFIG = "etc/pixels-spark.properties";

    public static final String RPC_HOST = "pixels.spark.rpc.host";
    public static final String RPC_PORT = "pixels.spark.rpc.port";
    public static final String METADATA_HOST = "pixels.spark.metadata.host";
    public static final String METADATA_PORT = "pixels.spark.metadata.port";
    public static final String CDC_BENCHMARK = "pixels.cdc.benchmark";
    public static final String DELTA_TARGET_PATH = "pixels.spark.delta.target.path";
    public static final String DELTA_CHECKPOINT_LOCATION = "pixels.spark.delta.checkpoint.location";
    public static final String CDC_EXECUTION_MODE = "pixels.spark.cdc.execution.mode";
    @Deprecated
    public static final String DELTA_MODE = "pixels.spark.delta.mode";
    public static final String DELTA_TRIGGER_MODE = "pixels.spark.delta.trigger.mode";
    public static final String DELTA_TRIGGER_INTERVAL = "pixels.spark.delta.trigger.interval";
    public static final String SINK_MODE = "pixels.spark.sink.mode";
    public static final String DELTA_NOOP_BUCKETS = "pixels.spark.delta.noop-buckets";
    public static final String DELTA_MAX_PENDING_BATCHES = "pixels.spark.delta.max-pending-batches";
    public static final String DELTA_ENABLE_LATEST_PER_PRIMARY_KEY = "pixels.spark.delta.enable-latest-per-primary-key";
    public static final String DELTA_AUTO_CREATE = "pixels.spark.delta.auto-create";
    public static final String DELTA_DELETE_MODE = "pixels.spark.delta.delete.mode";
    public static final String DELTA_ENABLE_DELETION_VECTORS = "pixels.spark.delta.enable-deletion-vectors";
    public static final String IMPORT_CSV_CHUNK_ROWS = "pixels.spark.import.csv.chunk-rows";
    public static final String IMPORT_COUNT_ROWS = "pixels.spark.import.count-rows";
    public static final String IMPORT_MODE = "pixels.spark.import.mode";
    public static final String SPARK_MASTER = "pixels.spark.master";
    public static final String SOURCE_MAX_ROWS_PER_BATCH = "pixels.spark.source.max-rows-per-batch";
    public static final String SOURCE_MAX_WAIT_MS_PER_BATCH = "pixels.spark.source.max-wait-ms-per-batch";
    public static final String SOURCE_REFRESH_MAX_WAIT_ON_NON_EMPTY_POLL =
            "pixels.spark.source.refresh-max-wait-on-non-empty-poll";
    public static final String SOURCE_EMPTY_POLL_SLEEP_MS = "pixels.spark.source.empty-poll-sleep-ms";

    private static final PixelsSparkConfig INSTANCE = new PixelsSparkConfig();

    private final ConfigFactory baseConfig;
    private final Properties sparkProperties;

    private PixelsSparkConfig()
    {
        this.baseConfig = ConfigFactory.Instance();
        this.sparkProperties = new Properties();
        loadSparkProperties();
    }

    public static PixelsSparkConfig instance()
    {
        return INSTANCE;
    }

    public String get(String key)
    {
        String value = sparkProperties.getProperty(key);
        if (value != null && !value.trim().isEmpty())
        {
            return value.trim();
        }

        value = baseConfig.getProperty(key);
        if (value != null && !value.trim().isEmpty())
        {
            return value.trim();
        }

        if (METADATA_HOST.equals(key))
        {
            return baseConfig.getProperty("metadata.server.host");
        }
        if (METADATA_PORT.equals(key))
        {
            return baseConfig.getProperty("metadata.server.port");
        }
        return null;
    }

    public String getOrDefault(String key, String defaultValue)
    {
        String value = get(key);
        return value != null ? value : defaultValue;
    }

    public int getIntOrDefault(String key, int defaultValue)
    {
        String value = get(key);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

    public boolean getBooleanOrDefault(String key, boolean defaultValue)
    {
        String value = get(key);
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }

    private void loadSparkProperties()
    {
        Path configPath = resolveConfigPath();
        if (configPath != null && Files.isRegularFile(configPath))
        {
            try (InputStream in = Files.newInputStream(configPath))
            {
                sparkProperties.load(in);
                return;
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed to load pixels-spark config: " + configPath, e);
            }
        }

        try (InputStream in = PixelsSparkConfig.class.getResourceAsStream("/pixels-spark.properties"))
        {
            if (in != null)
            {
                sparkProperties.load(in);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to load pixels-spark.properties from classpath", e);
        }
    }

    private Path resolveConfigPath()
    {
        String explicit = System.getenv(PIXELS_SPARK_CONFIG_ENV);
        if (explicit != null && !explicit.trim().isEmpty())
        {
            return Paths.get(explicit.trim());
        }

        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome != null && !pixelsHome.trim().isEmpty())
        {
            return Paths.get(pixelsHome, DEFAULT_PIXELS_SPARK_CONFIG);
        }
        return null;
    }
}
