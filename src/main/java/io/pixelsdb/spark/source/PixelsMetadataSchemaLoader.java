package io.pixelsdb.spark.source;

import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.spark.merge.PixelsDeltaMergeColumns;
import io.pixelsdb.spark.metadata.PixelsTableMetadata;
import io.pixelsdb.spark.metadata.PixelsTableMetadataRegistry;
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
        PixelsTableMetadata tableMetadata = PixelsTableMetadataRegistry.get(options)
                .getTableMetadata(options.getDatabase(), options.getTable());
        List<Column> columns = tableMetadata.getColumns();
        List<StructField> fields = new ArrayList<>(columns.size() + 5);
        for (Column column : columns)
        {
            fields.add(DataTypes.createStructField(
                    column.getName(),
                    PixelsTypeParser.parse(column.getType()),
                    true,
                    Metadata.empty()));
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
