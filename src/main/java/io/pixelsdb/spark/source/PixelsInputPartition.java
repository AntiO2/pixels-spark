package io.pixelsdb.spark.source;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class PixelsInputPartition implements InputPartition
{
    private final StructType schema;
    private final Map<String, String> options;
    private final long batchId;

    public PixelsInputPartition(StructType schema, Map<String, String> options, long batchId)
    {
        this.schema = schema;
        this.options = options;
        this.batchId = batchId;
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
}
