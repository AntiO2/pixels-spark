package io.pixelsdb.spark.source;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class PixelsScan implements Scan
{
    private final StructType schema;
    private final Map<String, String> options;

    public PixelsScan(StructType schema, Map<String, String> options)
    {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public StructType readSchema()
    {
        return schema;
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation)
    {
        return new PixelsMicroBatchStream(schema, options);
    }
}
