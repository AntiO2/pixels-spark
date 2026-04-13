package io.pixelsdb.spark.merge;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;

final class PixelsDeltaMergeContext
{
    private final SparkSession spark;
    private final PixelsDeltaMergeOptions options;
    private final StructType targetSchema;
    private final List<String> primaryKeys;
    private final String mergeCondition;

    PixelsDeltaMergeContext(
            SparkSession spark,
            PixelsDeltaMergeOptions options,
            StructType targetSchema,
            List<String> primaryKeys,
            String mergeCondition)
    {
        this.spark = spark;
        this.options = options;
        this.targetSchema = targetSchema;
        this.primaryKeys = primaryKeys;
        this.mergeCondition = mergeCondition;
    }

    SparkSession getSpark()
    {
        return spark;
    }

    PixelsDeltaMergeOptions getOptions()
    {
        return options;
    }

    StructType getTargetSchema()
    {
        return targetSchema;
    }

    List<String> getPrimaryKeys()
    {
        return primaryKeys;
    }

    String getMergeCondition()
    {
        return mergeCondition;
    }
}
