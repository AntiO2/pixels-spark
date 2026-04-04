package io.pixelsdb.spark.metadata;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.spark.source.PixelsSourceOptions;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class PixelsTableMetadataRegistry
{
    private static final ConcurrentMap<String, PixelsTableMetadataRegistry> INSTANCES = new ConcurrentHashMap<>();

    private final MetadataService metadataService;
    private final ConcurrentMap<SchemaTableName, PixelsTableMetadata> registry = new ConcurrentHashMap<>();

    private PixelsTableMetadataRegistry(MetadataService metadataService)
    {
        this.metadataService = metadataService;
    }

    public static PixelsTableMetadataRegistry get(PixelsSourceOptions options)
    {
        String cacheKey = buildCacheKey(options);
        return INSTANCES.computeIfAbsent(cacheKey, key ->
                new PixelsTableMetadataRegistry(createMetadataService(options)));
    }

    public PixelsTableMetadata getTableMetadata(String schemaName, String tableName)
    {
        SchemaTableName key = new SchemaTableName(schemaName, tableName);
        return registry.computeIfAbsent(key, ignored -> loadTableMetadata(schemaName, tableName));
    }

    private PixelsTableMetadata loadTableMetadata(String schemaName, String tableName)
    {
        try
        {
            Table table = metadataService.getTable(schemaName, tableName);
            SinglePointIndex primaryIndex = metadataService.getPrimaryIndex(table.getId());
            if (primaryIndex != null && !primaryIndex.isUnique())
            {
                throw new MetadataException("Non-unique primary index is not supported for "
                        + schemaName + "." + tableName);
            }
            List<Column> columns = metadataService.getColumns(schemaName, tableName, false);
            return new PixelsTableMetadata(table, primaryIndex, columns);
        }
        catch (MetadataException e)
        {
            throw new RuntimeException("Failed to load metadata for " + schemaName + "." + tableName, e);
        }
    }

    private static MetadataService createMetadataService(PixelsSourceOptions options)
    {
        if (options.getMetadataHost() != null && options.getMetadataPort() != null)
        {
            return MetadataService.CreateInstance(options.getMetadataHost(), options.getMetadataPort());
        }
        return MetadataService.Instance();
    }

    private static String buildCacheKey(PixelsSourceOptions options)
    {
        String host = options.getMetadataHost() != null ? options.getMetadataHost() : "__default__";
        String port = options.getMetadataPort() != null ? String.valueOf(options.getMetadataPort()) : "__default__";
        return host + ":" + port;
    }
}
