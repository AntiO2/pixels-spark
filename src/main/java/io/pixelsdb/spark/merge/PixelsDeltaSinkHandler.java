package io.pixelsdb.spark.merge;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

final class PixelsDeltaSinkHandler implements PixelsMergeSinkHandler
{
    @Override
    public void handle(PixelsDeltaPreparedBatch preparedBatch, PixelsDeltaMergeContext context, Long batchId)
    {
        applyHandlers(
                Arrays.asList(
                        new PixelsDeltaInsertHandler(),
                        new PixelsDeltaUpdateHandler(),
                        new PixelsDeltaDeleteHandler()),
                Arrays.asList(
                        preparedBatch.getInserts(),
                        preparedBatch.getUpdates(),
                        preparedBatch.getDeletes()),
                context);
    }

    private static void applyHandlers(
            List<PixelsDeltaOperationHandler> handlers,
            List<Dataset<Row>> operationBatches,
            PixelsDeltaMergeContext context)
    {
        for (int i = 0; i < handlers.size(); i++)
        {
            PixelsDeltaOperationHandler handler = handlers.get(i);
            Dataset<Row> rows = operationBatches.get(i);
            if (handler.canHandle(rows, context))
            {
                handler.handle(rows, context);
            }
        }
    }
}
