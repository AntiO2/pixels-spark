package io.pixelsdb.spark.source;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class PixelsScanBuilder implements ScanBuilder
{
    private final StructType schema;
    private final Map<String, String> options;

    public PixelsScanBuilder(StructType schema, Map<String, String> options)
    {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public Scan build()
    {
        return new PixelsScan(schema, options);
    }
}
