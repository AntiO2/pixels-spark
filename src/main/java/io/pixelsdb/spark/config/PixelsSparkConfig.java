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
    public static final String DELTA_TARGET_PATH = "pixels.spark.delta.target.path";
    public static final String DELTA_CHECKPOINT_LOCATION = "pixels.spark.delta.checkpoint.location";
    public static final String DELTA_TRIGGER_MODE = "pixels.spark.delta.trigger.mode";
    public static final String DELTA_TRIGGER_INTERVAL = "pixels.spark.delta.trigger.interval";
    public static final String DELTA_AUTO_CREATE = "pixels.spark.delta.auto-create";
    public static final String DELTA_DELETE_MODE = "pixels.spark.delta.delete.mode";
    public static final String DELTA_HASH_BUCKET_COUNT = "pixels.spark.delta.hash-bucket.count";
    public static final String SPARK_MASTER = "pixels.spark.master";

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
