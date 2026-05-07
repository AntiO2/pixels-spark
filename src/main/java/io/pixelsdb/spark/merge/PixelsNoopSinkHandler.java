package io.pixelsdb.spark.merge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PixelsNoopSinkHandler implements PixelsMergeSinkHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(PixelsNoopSinkHandler.class);

    @Override
    public void handle(PixelsDeltaPreparedBatch preparedBatch, PixelsDeltaMergeContext context, Long batchId)
    {
        LOG.info("sink-mode=noop batchId={} table={}.{} partitions={}",
                batchId,
                context.getOptions().getDatabase(),
                context.getOptions().getTable(),
                preparedBatch.getInserts().rdd().getNumPartitions());
    }
}
