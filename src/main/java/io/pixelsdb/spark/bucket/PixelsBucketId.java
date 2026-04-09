package io.pixelsdb.spark.bucket;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.spark.merge.PixelsDeltaMergeColumns;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;

public final class PixelsBucketId
{
    private static final ConcurrentMap<String, List<KeyFieldSpec>> SPEC_CACHE = new ConcurrentHashMap<>();

    private PixelsBucketId()
    {
    }

    public static Integer computeBucketId(Row pkRow, String specString, int configuredBucketCount)
    {
        if (configuredBucketCount <= 0)
        {
            return null;
        }
        List<KeyFieldSpec> specs = SPEC_CACHE.computeIfAbsent(specString, PixelsBucketId::parseSpecString);
        return computeBucketId(pkRow, specs, configuredBucketCount);
    }

    public static Dataset<Row> withBucketColumnFromMetadata(
            Dataset<Row> dataset,
            List<String> primaryKeys,
            List<Column> metadataColumns,
            int configuredBucketCount)
    {
        if (configuredBucketCount <= 0)
        {
            return dataset;
        }

        Map<String, Column> columnByName = new LinkedHashMap<>();
        for (Column column : metadataColumns)
        {
            columnByName.put(column.getName(), column);
        }

        List<KeyFieldSpec> specs = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys)
        {
            Column column = columnByName.get(primaryKey);
            if (column == null)
            {
                throw new IllegalArgumentException("Primary key column not found in metadata: " + primaryKey);
            }
            specs.add(KeyFieldSpec.fromPixelsColumn(primaryKey, column.getType()));
        }

        return withBucketColumn(dataset, specs, configuredBucketCount);
    }

    public static Dataset<Row> withBucketColumnFromSchema(
            Dataset<Row> dataset,
            List<String> primaryKeys,
            StructType schema,
            int configuredBucketCount)
    {
        if (configuredBucketCount <= 0)
        {
            return dataset;
        }

        Map<String, StructField> fieldByName = new LinkedHashMap<>();
        for (StructField field : schema.fields())
        {
            fieldByName.put(field.name(), field);
        }

        List<KeyFieldSpec> specs = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys)
        {
            StructField field = fieldByName.get(primaryKey);
            if (field == null)
            {
                throw new IllegalArgumentException("Primary key column not found in schema: " + primaryKey);
            }
            specs.add(KeyFieldSpec.fromSparkField(primaryKey, field.dataType()));
        }

        return withBucketColumn(dataset, specs, configuredBucketCount);
    }

    private static Dataset<Row> withBucketColumn(
            Dataset<Row> dataset,
            List<KeyFieldSpec> specs,
            int configuredBucketCount)
    {
        String udfName = "pixels_bucket_id_" + Integer.toUnsignedString(specs.hashCode()) + "_" + configuredBucketCount;
        UDFRegistration udfRegistration = dataset.sparkSession().udf();
        udfRegistration.register(udfName,
                (UDF1<Row, Integer>) pkRow -> computeBucketId(pkRow, specs, configuredBucketCount),
                DataTypes.IntegerType);

        org.apache.spark.sql.Column[] keyColumns = new org.apache.spark.sql.Column[specs.size()];
        for (int i = 0; i < specs.size(); i++)
        {
            keyColumns[i] = col(specs.get(i).name);
        }

        return dataset.withColumn(
                PixelsDeltaMergeColumns.BUCKET_ID,
                call_udf(udfName, struct(keyColumns)));
    }

    private static Integer computeBucketId(Row pkRow, List<KeyFieldSpec> specs, int configuredBucketCount)
    {
        if (pkRow == null)
        {
            return null;
        }

        int totalLength = 0;
        byte[][] parts = new byte[specs.size()][];
        for (int i = 0; i < specs.size(); i++)
        {
            Object value = pkRow.get(i);
            if (value == null)
            {
                throw new IllegalArgumentException("Primary key column is null: " + specs.get(i).name);
            }
            parts[i] = specs.get(i).serialize(value);
            totalLength += parts[i].length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        for (byte[] part : parts)
        {
            buffer.put(part);
        }

        int bucketId = RetinaUtils.getBucketIdFromByteBuffer(ByteString.copyFrom((ByteBuffer) buffer.rewind()));
        return Math.floorMod(bucketId, configuredBucketCount);
    }

    private static List<KeyFieldSpec> parseSpecString(String specString)
    {
        if (specString == null || specString.trim().isEmpty())
        {
            return Collections.emptyList();
        }

        List<KeyFieldSpec> specs = new ArrayList<>();
        for (String token : specString.split(";"))
        {
            String trimmed = token.trim();
            if (trimmed.isEmpty())
            {
                continue;
            }

            String[] parts = trimmed.split("\\|", -1);
            if (parts.length != 4)
            {
                throw new IllegalArgumentException("Invalid bucket spec token: " + trimmed);
            }
            specs.add(new KeyFieldSpec(
                    parts[0],
                    parts[1],
                    Integer.parseInt(parts[2]),
                    Integer.parseInt(parts[3])));
        }
        return specs;
    }

    private static final class KeyFieldSpec implements Serializable
    {
        private final String name;
        private final String kind;
        private final int precision;
        private final int scale;

        private KeyFieldSpec(String name, String kind, int precision, int scale)
        {
            this.name = name;
            this.kind = kind;
            this.precision = precision;
            this.scale = scale;
        }

        private static KeyFieldSpec fromPixelsColumn(String name, String typeName)
        {
            TypeDescription type = TypeDescription.fromString(typeName);
            return new KeyFieldSpec(name, type.getCategory().name(), type.getPrecision(), type.getScale());
        }

        private static KeyFieldSpec fromSparkField(String name, DataType dataType)
        {
            if (dataType.sameType(DataTypes.BooleanType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.BOOLEAN.name(), 0, 0);
            }
            if (dataType.sameType(DataTypes.ByteType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.BYTE.name(), 0, 0);
            }
            if (dataType.sameType(DataTypes.ShortType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.SHORT.name(), 0, 0);
            }
            if (dataType.sameType(DataTypes.IntegerType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.INT.name(), 0, 0);
            }
            if (dataType.sameType(DataTypes.LongType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.LONG.name(), 0, 0);
            }
            if (dataType.sameType(DataTypes.FloatType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.FLOAT.name(), 0, 0);
            }
            if (dataType.sameType(DataTypes.DoubleType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.DOUBLE.name(), 0, 0);
            }
            if (dataType.sameType(DataTypes.StringType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.STRING.name(), 0, 0);
            }
            if (dataType.sameType(DataTypes.DateType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.DATE.name(), 0, 0);
            }
            if (dataType.sameType(DataTypes.TimestampType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.TIMESTAMP.name(), 0, 0);
            }
            if (dataType instanceof DecimalType)
            {
                DecimalType decimalType = (DecimalType) dataType;
                return new KeyFieldSpec(name, TypeDescription.Category.DECIMAL.name(),
                        decimalType.precision(), decimalType.scale());
            }
            if (dataType instanceof BinaryType || dataType.sameType(DataTypes.BinaryType))
            {
                return new KeyFieldSpec(name, TypeDescription.Category.BINARY.name(), 0, 0);
            }

            throw new UnsupportedOperationException("Unsupported primary key type: " + dataType.catalogString());
        }

        private byte[] serialize(Object value)
        {
            TypeDescription.Category category = TypeDescription.Category.valueOf(kind);
            switch (category)
            {
                case BOOLEAN:
                    if (value instanceof Boolean)
                    {
                        return new byte[]{((Boolean) value) ? (byte) 1 : (byte) 0};
                    }
                    return new byte[]{((Number) value).byteValue()};
                case BYTE:
                    return new byte[]{((Number) value).byteValue()};
                case SHORT:
                case INT:
                case DATE:
                case TIME:
                    return ByteBuffer.allocate(Integer.BYTES).putInt(toInt(value, category)).array();
                case LONG:
                    return ByteBuffer.allocate(Long.BYTES).putLong(((Number) value).longValue()).array();
                case TIMESTAMP:
                    return ByteBuffer.allocate(Long.BYTES).putLong(toTimestampMicros(value)).array();
                case FLOAT:
                    return ByteBuffer.allocate(Integer.BYTES).putInt(Float.floatToIntBits(((Number) value).floatValue())).array();
                case DOUBLE:
                    return ByteBuffer.allocate(Long.BYTES).putLong(Double.doubleToLongBits(((Number) value).doubleValue())).array();
                case DECIMAL:
                    return serializeDecimal(value);
                case CHAR:
                case VARCHAR:
                case STRING:
                case BINARY:
                case VARBINARY:
                    return toBytes(value);
                default:
                    throw new UnsupportedOperationException("Unsupported primary key category: " + category);
            }
        }

        private int toInt(Object value, TypeDescription.Category category)
        {
            if (category == TypeDescription.Category.DATE)
            {
                if (value instanceof Date)
                {
                    return (int) ((Date) value).toLocalDate().toEpochDay();
                }
                if (value instanceof LocalDate)
                {
                    return (int) ((LocalDate) value).toEpochDay();
                }
            }
            return ((Number) value).intValue();
        }

        private long toTimestampMicros(Object value)
        {
            if (value instanceof Timestamp)
            {
                Timestamp ts = (Timestamp) value;
                long seconds = Math.floorDiv(ts.getTime(), 1000L);
                return seconds * 1_000_000L + ts.getNanos() / 1000L;
            }
            if (value instanceof Number)
            {
                return ((Number) value).longValue();
            }
            throw new IllegalArgumentException("Unsupported timestamp value: " + value.getClass());
        }

        private byte[] serializeDecimal(Object value)
        {
            BigDecimal decimal = (value instanceof BigDecimal)
                    ? (BigDecimal) value
                    : new BigDecimal(String.valueOf(value));
            if (decimal.scale() != scale)
            {
                decimal = decimal.setScale(scale, BigDecimal.ROUND_HALF_UP);
            }

            if (precision <= TypeDescription.MAX_SHORT_DECIMAL_PRECISION)
            {
                return ByteBuffer.allocate(Long.BYTES).putLong(decimal.unscaledValue().longValue()).array();
            }

            BigInteger unscaledValue = decimal.unscaledValue();
            long high = unscaledValue.shiftRight(64).longValue();
            long low = unscaledValue.longValue();
            return ByteBuffer.allocate(16).putLong(high).putLong(low).array();
        }

        private byte[] toBytes(Object value)
        {
            if (value instanceof byte[])
            {
                return Arrays.copyOf((byte[]) value, ((byte[]) value).length);
            }
            return String.valueOf(value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
