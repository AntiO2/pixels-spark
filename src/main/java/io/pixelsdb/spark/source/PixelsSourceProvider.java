package io.pixelsdb.spark.source;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class PixelsSourceProvider implements TableProvider, DataSourceRegister
{
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options)
    {
        return PixelsMetadataSchemaLoader.load(new PixelsSourceOptions(options));
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties)
    {
        PixelsSourceOptions options = new PixelsSourceOptions(properties);
        StructType resolvedSchema = schema != null ? schema : PixelsMetadataSchemaLoader.load(options);
        return new PixelsTable(resolvedSchema, properties);
    }

    @Override
    public boolean supportsExternalMetadata()
    {
        return true;
    }

    @Override
    public String shortName()
    {
        return "pixels";
    }
}
