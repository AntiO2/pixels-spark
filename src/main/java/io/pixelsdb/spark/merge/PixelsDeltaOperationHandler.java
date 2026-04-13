package io.pixelsdb.spark.merge;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

interface PixelsDeltaOperationHandler
{
    boolean canHandle(Dataset<Row> rows, PixelsDeltaMergeContext context);

    void handle(Dataset<Row> rows, PixelsDeltaMergeContext context);
}
