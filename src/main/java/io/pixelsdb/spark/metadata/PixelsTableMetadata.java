package io.pixelsdb.spark.metadata;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PixelsTableMetadata
{
    private final Table table;
    private final SinglePointIndex primaryIndex;
    private final List<Column> columns;
    private final List<String> primaryKeyColumns;

    public PixelsTableMetadata(Table table, SinglePointIndex primaryIndex, List<Column> columns) throws MetadataException
    {
        this.table = table;
        this.primaryIndex = primaryIndex;
        this.columns = columns;
        this.primaryKeyColumns = new ArrayList<>();

        if (primaryIndex != null)
        {
            Map<Long, Column> columnById = new HashMap<>();
            for (Column column : columns)
            {
                columnById.put(column.getId(), column);
            }

            for (Integer keyColumnId : primaryIndex.getKeyColumns().getKeyColumnIds())
            {
                Column column = columnById.get(keyColumnId.longValue());
                if (column == null)
                {
                    throw new MetadataException("Primary key column id " + keyColumnId
                            + " not found in table " + table.getName());
                }
                primaryKeyColumns.add(column.getName());
            }
        }
    }

    public boolean hasPrimaryKey()
    {
        return primaryIndex != null && !primaryKeyColumns.isEmpty();
    }

    public Table getTable()
    {
        return table;
    }

    public SinglePointIndex getPrimaryIndex()
    {
        return primaryIndex;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public List<String> getPrimaryKeyColumns()
    {
        return primaryKeyColumns;
    }
}
