package io.pixelsdb.spark.source;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.spark.merge.PixelsDeltaMergeColumns;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

public class PixelsRowRecordDecoder
{
    private final StructType schema;
    private final int bucketId;

    public PixelsRowRecordDecoder(StructType schema, int bucketId)
    {
        this.schema = schema;
        this.bucketId = bucketId;
    }

    public InternalRow decode(SinkProto.RowRecord rowRecord)
    {
        Object[] row = decodeValues(rowRecord, true);
        if (row == null)
        {
            return null;
        }
        return new GenericInternalRow(row);
    }

    public Row decodeExternal(SinkProto.RowRecord rowRecord)
    {
        Object[] row = decodeValues(rowRecord, false);
        if (row == null)
        {
            return null;
        }
        return RowFactory.create(row);
    }

    private Object[] decodeValues(SinkProto.RowRecord rowRecord, boolean internal)
    {
        SinkProto.OperationType op = rowRecord.getOp();
        SinkProto.RowValue rowValue = selectRowValue(rowRecord);
        if (rowValue == null)
        {
            return null;
        }

        List<SinkProto.ColumnValue> values = rowValue.getValuesList();
        StructField[] fields = schema.fields();
        Object[] row = new Object[fields.length];

        int valueIndex = 0;
        for (int i = 0; i < fields.length; i++)
        {
            StructField field = fields[i];
            if (PixelsDeltaMergeColumns.BUCKET_ID.equalsIgnoreCase(field.name()))
            {
                row[i] = bucketId;
                continue;
            }
            if (PixelsMetadataColumns.OP.equalsIgnoreCase(field.name()))
            {
                row[i] = internal ? UTF8String.fromString(op.name()) : op.name();
                continue;
            }
            if (PixelsMetadataColumns.TXN_ID.equalsIgnoreCase(field.name()))
            {
                row[i] = rowRecord.hasTransaction()
                        ? (internal
                            ? UTF8String.fromString(rowRecord.getTransaction().getId())
                            : rowRecord.getTransaction().getId())
                        : null;
                continue;
            }
            if (PixelsMetadataColumns.TOTAL_ORDER.equalsIgnoreCase(field.name()))
            {
                row[i] = rowRecord.hasTransaction() ? rowRecord.getTransaction().getTotalOrder() : null;
                continue;
            }
            if (PixelsMetadataColumns.DATA_COLLECTION_ORDER.equalsIgnoreCase(field.name()))
            {
                row[i] = rowRecord.hasTransaction() ? rowRecord.getTransaction().getDataCollectionOrder() : null;
                continue;
            }

            if (valueIndex >= values.size())
            {
                row[i] = null;
            }
            else
            {
                row[i] = internal
                        ? parseInternalValue(values.get(valueIndex).getValue(), field.dataType())
                        : parseExternalValue(values.get(valueIndex).getValue(), field.dataType());
                valueIndex++;
            }
        }
        return row;
    }

    private SinkProto.RowValue selectRowValue(SinkProto.RowRecord rowRecord)
    {
        switch (rowRecord.getOp())
        {
            case INSERT:
            case SNAPSHOT:
                return rowRecord.hasAfter() ? rowRecord.getAfter() : null;
            case UPDATE:
                if (rowRecord.hasAfter())
                {
                    return rowRecord.getAfter();
                }
                return rowRecord.hasBefore() ? rowRecord.getBefore() : null;
            case DELETE:
                return rowRecord.hasBefore() ? rowRecord.getBefore() : null;
            default:
                return null;
        }
    }

    private Object parseInternalValue(ByteString byteString, DataType dataType)
    {
        if (byteString == null || byteString.isEmpty())
        {
            return null;
        }

        byte[] bytes = byteString.toByteArray();
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        String typeName = dataType.typeName();

        switch (typeName)
        {
            case "string":
            case "char":
            case "varchar":
                return UTF8String.fromString(byteString.toStringUtf8());
            case "binary":
                return bytes;
            case "integer":
            case "date":
                return buffer.getInt();
            case "long":
                return buffer.getLong();
            case "timestamp":
                // Pixels stores timestamps as epoch microseconds, which matches Spark's
                // internal representation for TimestampType.
                return buffer.getLong();
            case "float":
                return Float.intBitsToFloat(buffer.getInt());
            case "double":
                return Double.longBitsToDouble(buffer.getLong());
            case "boolean":
                return Boolean.parseBoolean(byteString.toStringUtf8());
            case "decimal":
                BigDecimal decimalValue = new BigDecimal(byteString.toStringUtf8());
                DecimalType decimalType = (DecimalType) dataType;
                return Decimal.apply(decimalValue, decimalType.precision(), decimalType.scale());
            default:
                if (dataType.sameType(DataTypes.IntegerType))
                {
                    return buffer.getInt();
                }
                if (dataType.sameType(DataTypes.LongType))
                {
                    return buffer.getLong();
                }
                if (dataType.sameType(DataTypes.FloatType))
                {
                    return Float.intBitsToFloat(buffer.getInt());
                }
                if (dataType.sameType(DataTypes.DoubleType))
                {
                    return Double.longBitsToDouble(buffer.getLong());
                }
                if (dataType.sameType(DataTypes.BooleanType))
                {
                    return Boolean.parseBoolean(byteString.toStringUtf8());
                }
                throw new UnsupportedOperationException("Unsupported Spark type: " + dataType.catalogString());
        }
    }

    private Object parseExternalValue(ByteString byteString, DataType dataType)
    {
        Object internalValue = parseInternalValue(byteString, dataType);
        if (internalValue == null)
        {
            return null;
        }

        String typeName = dataType.typeName();
        switch (typeName)
        {
            case "string":
            case "char":
            case "varchar":
                return internalValue.toString();
            case "timestamp":
                long micros = (Long) internalValue;
                long millis = micros / 1000L;
                Timestamp timestamp = new Timestamp(millis);
                timestamp.setNanos((int) ((micros % 1_000_000L) * 1000L));
                return timestamp;
            case "date":
                int days = (Integer) internalValue;
                return Date.valueOf(LocalDate.ofEpochDay(days));
            case "decimal":
                return ((Decimal) internalValue).toJavaBigDecimal();
            default:
                return internalValue;
        }
    }
}
