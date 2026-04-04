package io.pixelsdb.spark.source;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PixelsTypeParser
{
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("decimal\\((\\d+)\\s*,\\s*(\\d+)\\)");
    private static final Pattern CHAR_PATTERN = Pattern.compile("char\\((\\d+)\\)");
    private static final Pattern VARCHAR_PATTERN = Pattern.compile("varchar\\((\\d+)\\)");
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("timestamp\\((\\d+)\\)");

    private PixelsTypeParser()
    {
    }

    public static DataType parse(String pixelsType)
    {
        String normalized = pixelsType.trim().toLowerCase(Locale.ROOT);

        Matcher decimalMatcher = DECIMAL_PATTERN.matcher(normalized);
        if (decimalMatcher.matches())
        {
            int precision = Integer.parseInt(decimalMatcher.group(1));
            int scale = Integer.parseInt(decimalMatcher.group(2));
            return DataTypes.createDecimalType(precision, scale);
        }

        if (CHAR_PATTERN.matcher(normalized).matches() || VARCHAR_PATTERN.matcher(normalized).matches())
        {
            return DataTypes.StringType;
        }

        Matcher timestampMatcher = TIMESTAMP_PATTERN.matcher(normalized);
        if (timestampMatcher.matches())
        {
            return DataTypes.TimestampType;
        }

        switch (normalized)
        {
            case "int":
            case "integer":
                return DataTypes.IntegerType;
            case "bigint":
            case "long":
                return DataTypes.LongType;
            case "smallint":
                return DataTypes.ShortType;
            case "tinyint":
                return DataTypes.ByteType;
            case "float":
            case "real":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            case "boolean":
                return DataTypes.BooleanType;
            case "string":
            case "binary":
            case "varbinary":
                return "binary".equals(normalized) || "varbinary".equals(normalized)
                        ? DataTypes.BinaryType
                        : DataTypes.StringType;
            case "date":
                return DataTypes.DateType;
            case "timestamp":
                return DataTypes.TimestampType;
            default:
                throw new IllegalArgumentException("Unsupported Pixels column type: " + pixelsType);
        }
    }
}
