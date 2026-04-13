package io.pixelsdb.spark.source;

import io.pixelsdb.spark.benchmark.BenchmarkTableDefinition;
import io.pixelsdb.spark.benchmark.BenchmarkTableRegistry;
import io.pixelsdb.spark.merge.PixelsDeltaMergeColumns;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public final class PixelsMetadataSchemaLoader
{
    private PixelsMetadataSchemaLoader()
    {
    }

    public static StructType load(PixelsSourceOptions options)
    {
        BenchmarkTableDefinition definition = BenchmarkTableRegistry.require(
                options.getBenchmark(),
                options.getTable());
        List<StructField> fields = new ArrayList<>(definition.getSchema().fields().length + 5);
        for (StructField field : definition.getSchema().fields())
        {
            fields.add(DataTypes.createStructField(field.name(), field.dataType(), field.nullable(), Metadata.empty()));
        }
        fields.add(DataTypes.createStructField(
                PixelsDeltaMergeColumns.BUCKET_ID,
                DataTypes.IntegerType,
                false,
                Metadata.empty()));
        fields.add(DataTypes.createStructField(PixelsMetadataColumns.OP, DataTypes.StringType, true, Metadata.empty()));
        fields.add(DataTypes.createStructField(PixelsMetadataColumns.TXN_ID, DataTypes.StringType, true, Metadata.empty()));
        fields.add(DataTypes.createStructField(PixelsMetadataColumns.TOTAL_ORDER, DataTypes.LongType, true, Metadata.empty()));
        fields.add(DataTypes.createStructField(PixelsMetadataColumns.DATA_COLLECTION_ORDER, DataTypes.LongType, true, Metadata.empty()));
        return DataTypes.createStructType(fields);
    }
}
