package io.pixelsdb.spark.merge;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

final class PixelsDeltaInsertHandler implements PixelsDeltaOperationHandler
{
    @Override
    public boolean canHandle(Dataset<Row> rows, PixelsDeltaMergeContext context)
    {
        return !rows.isEmpty();
    }

    @Override
    public void handle(Dataset<Row> rows, PixelsDeltaMergeContext context)
    {
        PixelsDeltaPreparedBatchFactory.prepareInsertRows(rows, context.getTargetSchema())
                .write()
                .format("delta")
                .mode("append")
                .save(context.getOptions().getTargetPath());
    }
}
