package io.pixelsdb.spark.merge;

import io.pixelsdb.spark.source.PixelsMetadataColumns;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

final class PixelsMergeSupport
{
    private PixelsMergeSupport()
    {
    }

    static StructType targetSchema(StructType schema, PixelsDeltaMergeOptions options)
    {
        List<StructField> fields = new ArrayList<>();
        for (StructField field : schema.fields())
        {
            if (!PixelsMetadataColumns.isMetadataColumn(field.name()))
            {
                if (options.isHudiSinkMode() && PixelsDeltaMergeColumns.BUCKET_ID.equals(field.name()))
                {
                    continue;
                }
                fields.add(field);
            }
        }
        if (isSoftDelete(options))
        {
            fields.add(DataTypes.createStructField(PixelsDeltaMergeColumns.IS_DELETED, DataTypes.BooleanType, false));
            fields.add(DataTypes.createStructField(PixelsDeltaMergeColumns.DELETED_AT, DataTypes.TimestampType, true));
        }
        return new StructType(fields.toArray(new StructField[0]));
    }

    static String buildMergeCondition(List<String> primaryKeys)
    {
        return buildMergeCondition(primaryKeys, new ArrayList<String>(), true, true);
    }

    static String buildMergeCondition(
            List<String> primaryKeys,
            List<String> extraConditionColumns,
            boolean includeBucketColumn)
    {
        return buildMergeCondition(primaryKeys, extraConditionColumns, includeBucketColumn, true);
    }

    static String buildMergeCondition(
            List<String> primaryKeys,
            List<String> extraConditionColumns,
            boolean includeBucketColumn,
            boolean useNullSafeEquals)
    {
        Set<String> conditionColumns = new LinkedHashSet<>();
        conditionColumns.addAll(primaryKeys);
        conditionColumns.addAll(extraConditionColumns);
        if (includeBucketColumn)
        {
            conditionColumns.add(PixelsDeltaMergeColumns.BUCKET_ID);
        }

        StringBuilder builder = new StringBuilder();
        int index = 0;
        for (String column : conditionColumns)
        {
            if (index++ > 0)
            {
                builder.append(" AND ");
            }
            builder.append("t.")
                    .append(column)
                    .append(useNullSafeEquals ? " <=> " : " = ")
                    .append("s.")
                    .append(column);
        }
        return builder.toString();
    }

    static boolean isHardDelete(PixelsDeltaMergeOptions options)
    {
        return !"ignore".equalsIgnoreCase(options.getDeleteMode())
                && !"soft".equalsIgnoreCase(options.getDeleteMode());
    }

    static boolean isSoftDelete(PixelsDeltaMergeOptions options)
    {
        return "soft".equalsIgnoreCase(options.getDeleteMode());
    }
}
