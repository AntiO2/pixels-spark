package io.pixelsdb.spark.benchmark;

import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;

public final class BenchmarkTableDefinition
{
    private final String benchmark;
    private final String tableName;
    private final String fileName;
    private final String delimiter;
    private final StructType schema;
    private final List<String> primaryKeyColumns;

    public BenchmarkTableDefinition(
            String benchmark,
            String tableName,
            String fileName,
            String delimiter,
            StructType schema,
            List<String> primaryKeyColumns)
    {
        this.benchmark = benchmark;
        this.tableName = tableName;
        this.fileName = fileName;
        this.delimiter = delimiter;
        this.schema = schema;
        this.primaryKeyColumns = Collections.unmodifiableList(primaryKeyColumns);
    }

    public String getBenchmark()
    {
        return benchmark;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getFileName()
    {
        return fileName;
    }

    public String getDelimiter()
    {
        return delimiter;
    }

    public StructType getSchema()
    {
        return schema;
    }

    public List<String> getPrimaryKeyColumns()
    {
        return primaryKeyColumns;
    }
}
