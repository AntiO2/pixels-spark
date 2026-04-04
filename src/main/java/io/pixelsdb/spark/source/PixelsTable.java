package io.pixelsdb.spark.source;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class PixelsTable implements Table, SupportsRead
{
    private final StructType schema;
    private final Map<String, String> options;

    public PixelsTable(StructType schema, Map<String, String> options)
    {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public String name()
    {
        return "pixels";
    }

    @Override
    public StructType schema()
    {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities()
    {
        return Collections.singleton(TableCapability.MICRO_BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options)
    {
        return new PixelsScanBuilder(schema, this.options);
    }
}
