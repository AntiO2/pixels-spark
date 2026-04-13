package io.pixelsdb.spark.benchmark;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class BenchmarkTableRegistry
{
    public static final String BENCHMARK_HYBENCH = "hybench";
    public static final String BENCHMARK_CHBENCHMARK = "chbenchmark";

    private static final ConcurrentMap<String, Map<String, BenchmarkTableDefinition>> REGISTRY =
            new ConcurrentHashMap<>();

    private BenchmarkTableRegistry()
    {
    }

    public static String normalizeBenchmark(String benchmark)
    {
        if (benchmark == null || benchmark.trim().isEmpty())
        {
            return BENCHMARK_HYBENCH;
        }
        return benchmark.trim().toLowerCase(Locale.ROOT);
    }

    public static BenchmarkTableDefinition require(String benchmark, String tableName)
    {
        Map<String, BenchmarkTableDefinition> definitions = definitions(normalizeBenchmark(benchmark));
        BenchmarkTableDefinition definition = definitions.get(normalizeTableName(tableName));
        if (definition == null)
        {
            throw new IllegalArgumentException("Table " + tableName + " is not defined for benchmark " + benchmark);
        }
        return definition;
    }

    public static List<BenchmarkTableDefinition> list(String benchmark)
    {
        return new ArrayList<>(definitions(normalizeBenchmark(benchmark)).values());
    }

    private static Map<String, BenchmarkTableDefinition> definitions(String benchmark)
    {
        return REGISTRY.computeIfAbsent(benchmark, BenchmarkTableRegistry::loadDefinitions);
    }

    private static Map<String, BenchmarkTableDefinition> loadDefinitions(String benchmark)
    {
        String resourcePath = "/benchmarks/" + benchmark + ".properties";
        Properties properties = new Properties();
        try (InputStream in = BenchmarkTableRegistry.class.getResourceAsStream(resourcePath))
        {
            if (in == null)
            {
                throw new IllegalArgumentException("Missing benchmark definition resource: " + resourcePath);
            }
            properties.load(in);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to load benchmark definition resource: " + resourcePath, e);
        }

        String tablesValue = properties.getProperty("tables");
        if (tablesValue == null || tablesValue.trim().isEmpty())
        {
            throw new IllegalArgumentException("Missing tables property in " + resourcePath);
        }

        Map<String, BenchmarkTableDefinition> definitions = new LinkedHashMap<>();
        for (String table : splitCsv(tablesValue))
        {
            String prefix = "table." + table + ".";
            String fileName = requireProperty(properties, prefix + "file", resourcePath);
            String delimiter = properties.getProperty(prefix + "delimiter", ",");
            String schemaValue = requireProperty(properties, prefix + "schema", resourcePath);
            String primaryKeysValue = requireProperty(properties, prefix + "primary-keys", resourcePath);
            BenchmarkTableDefinition definition = new BenchmarkTableDefinition(
                    benchmark,
                    table,
                    fileName,
                    delimiter,
                    parseSchema(schemaValue),
                    splitCsv(primaryKeysValue));
            definitions.put(normalizeTableName(table), definition);
        }
        return Collections.unmodifiableMap(definitions);
    }

    private static String requireProperty(Properties properties, String key, String resourcePath)
    {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty())
        {
            throw new IllegalArgumentException("Missing property " + key + " in " + resourcePath);
        }
        return value.trim();
    }

    private static StructType parseSchema(String schemaValue)
    {
        List<StructField> fields = new ArrayList<>();
        for (String columnSpec : splitCsv(schemaValue))
        {
            int separator = columnSpec.indexOf(':');
            if (separator <= 0 || separator == columnSpec.length() - 1)
            {
                throw new IllegalArgumentException("Invalid column definition: " + columnSpec);
            }
            String columnName = columnSpec.substring(0, separator).trim();
            String typeName = columnSpec.substring(separator + 1).trim();
            fields.add(DataTypes.createStructField(columnName, parseType(typeName), true, Metadata.empty()));
        }
        return DataTypes.createStructType(fields);
    }

    private static DataType parseType(String typeName)
    {
        switch (typeName.toLowerCase(Locale.ROOT))
        {
            case "int":
            case "integer":
                return DataTypes.IntegerType;
            case "long":
            case "bigint":
                return DataTypes.LongType;
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            case "string":
                return DataTypes.StringType;
            case "date":
                return DataTypes.DateType;
            case "timestamp":
                return DataTypes.TimestampType;
            case "boolean":
                return DataTypes.BooleanType;
            default:
                throw new IllegalArgumentException("Unsupported Spark type in benchmark definition: " + typeName);
        }
    }

    private static List<String> splitCsv(String value)
    {
        if (value == null || value.trim().isEmpty())
        {
            return Collections.emptyList();
        }
        List<String> results = new ArrayList<>();
        Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(part -> !part.isEmpty())
                .forEach(results::add);
        return results;
    }

    private static String normalizeTableName(String tableName)
    {
        return tableName == null ? "" : tableName.trim().toLowerCase(Locale.ROOT);
    }
}
