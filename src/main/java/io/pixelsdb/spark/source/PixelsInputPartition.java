package io.pixelsdb.spark.source;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedHashMap;
import java.util.Map;

public class PixelsInputPartition implements InputPartition
{
    private final StructType schema;
    private final Map<String, String> options;
    private final long batchId;
    private final Integer bucketId;

    public PixelsInputPartition(StructType schema, Map<String, String> options, long batchId)
    {
        this(schema, options, batchId, null);
    }

    public PixelsInputPartition(StructType schema, Map<String, String> options, long batchId, Integer bucketId)
    {
        this.schema = schema;
        this.options = withBucketOverride(options, bucketId);
        this.batchId = batchId;
        this.bucketId = bucketId;
    }

    public StructType getSchema()
    {
        return schema;
    }

    public Map<String, String> getOptions()
    {
        return options;
    }

    public long getBatchId()
    {
        return batchId;
    }

    public Integer getBucketId()
    {
        return bucketId;
    }

    private static Map<String, String> withBucketOverride(Map<String, String> options, Integer bucketId)
    {
        if (bucketId == null)
        {
            return options;
        }

        Map<String, String> overridden = new LinkedHashMap<>(options);
        overridden.put(PixelsSourceOptions.BUCKETS, String.valueOf(bucketId));
        return overridden;
    }
}
