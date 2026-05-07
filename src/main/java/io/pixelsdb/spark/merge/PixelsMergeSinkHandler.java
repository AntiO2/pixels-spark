package io.pixelsdb.spark.merge;

interface PixelsMergeSinkHandler
{
    void handle(PixelsDeltaPreparedBatch preparedBatch, PixelsDeltaMergeContext context, Long batchId);
}
