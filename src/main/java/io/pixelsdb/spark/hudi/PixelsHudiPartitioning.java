package io.pixelsdb.spark.hudi;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.hash;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;

public final class PixelsHudiPartitioning
{
    public static final String HUDI_PARTITION_COLUMN = "_pixels_hudi_partition";

    private PixelsHudiPartitioning()
    {
    }

    public static int resolvePartitionCount(int bucketCount)
    {
        return Math.max(1, bucketCount);
    }

    public static Dataset<Row> withPartitionColumn(
            Dataset<Row> dataset,
            List<String> primaryKeys,
            int partitionCount)
    {
        if (dataset == null)
        {
            throw new IllegalArgumentException("dataset is null");
        }
        if (primaryKeys == null || primaryKeys.isEmpty())
        {
            throw new IllegalArgumentException("primaryKeys must not be empty");
        }

        int effectivePartitionCount = Math.max(1, partitionCount);
        List<Column> hashInputs = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys)
        {
            hashInputs.add(coalesce(col(primaryKey).cast("string"), lit("")));
        }

        Column hashValue = hash(hashInputs.toArray(new Column[0]));
        return dataset.withColumn(
                HUDI_PARTITION_COLUMN,
                pmod(hashValue, lit(effectivePartitionCount)).cast("int"));
    }

    public static StructType withPartitionColumnInSchema(StructType schema)
    {
        if (schema.getFieldIndex(HUDI_PARTITION_COLUMN).isDefined())
        {
            return schema;
        }
        List<StructField> fields = new ArrayList<>();
        for (StructField field : schema.fields())
        {
            fields.add(field);
        }
        fields.add(DataTypes.createStructField(HUDI_PARTITION_COLUMN, DataTypes.IntegerType, false));
        return new StructType(fields.toArray(new StructField[0]));
    }
}
