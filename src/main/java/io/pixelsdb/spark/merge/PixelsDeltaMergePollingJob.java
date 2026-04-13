package io.pixelsdb.spark.merge;

import io.pixelsdb.spark.config.PixelsSparkConfig;
import io.pixelsdb.spark.source.PixelsPollingBatchFetcher;
import io.pixelsdb.spark.source.PixelsSourceOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class PixelsDeltaMergePollingJob
{
    private static final Logger LOG = LoggerFactory.getLogger(PixelsDeltaMergePollingJob.class);

    private PixelsDeltaMergePollingJob()
    {
    }

    public static void run(SparkSession spark, PixelsDeltaMergeOptions options)
    {
        PixelsSourceOptions sourceOptions = new PixelsSourceOptions(toSourceOptions(options));
        List<Integer> buckets = PixelsPollingBatchFetcher.buckets(sourceOptions);
        int maxPendingBatches = Math.max(1, PixelsSparkConfig.instance()
                .getIntOrDefault(PixelsSparkConfig.DELTA_MAX_PENDING_BATCHES, 8));
        BlockingQueue<EmittedBatch> emittedBatches = new LinkedBlockingQueue<>(maxPendingBatches);
        List<Thread> workers = new ArrayList<>(buckets.size());
        WorkerState[] states = new WorkerState[buckets.size()];

        LOG.info("pollingQueue table={}.{} maxPendingBatches={} buckets={}",
                options.getDatabase(), options.getTable(), maxPendingBatches, buckets.size());

        for (int i = 0; i < buckets.size(); i++)
        {
            int bucketId = buckets.get(i);
            WorkerState state = new WorkerState(bucketId);
            states[i] = state;
            Thread worker = new Thread(() -> runBucketWorker(sourceOptions, state, emittedBatches, isOneShot(options)),
                    "pixels-poll-" + options.getTable() + "-bucket-" + bucketId);
            worker.setDaemon(true);
            worker.start();
            workers.add(worker);
        }

        long batchId = 0L;
        try
        {
            while (!Thread.currentThread().isInterrupted())
            {
                EmittedBatch emitted = emittedBatches.poll(200, TimeUnit.MILLISECONDS);
                if (emitted != null)
                {
                    Dataset<Row> batch = PixelsPollingBatchFetcher.toDataset(spark, sourceOptions, emitted.rows);
                    String effectiveSinkMode = options.sinkModeForBucket(emitted.bucketId);
                    PixelsDeltaMergeOptions bucketOptions = effectiveSinkMode.equals(options.getSinkMode())
                            ? options
                            : options.withSinkMode(effectiveSinkMode);
                    LOG.info("pollingProcess table={}.{} bucket={} batchId={} rows={} sinkMode={}",
                            options.getDatabase(), options.getTable(), emitted.bucketId, batchId,
                            emitted.rows.size(), effectiveSinkMode);
                    PixelsDeltaMergeJob.processBatch(batch, batchId++, bucketOptions);
                }

                if (isOneShot(options) && allWorkersStopped(states) && emittedBatches.isEmpty())
                {
                    return;
                }
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            for (Thread worker : workers)
            {
                worker.interrupt();
            }
        }
    }

    private static void runBucketWorker(
            PixelsSourceOptions sourceOptions,
            WorkerState state,
            BlockingQueue<EmittedBatch> emittedBatches,
            boolean oneShot)
    {
        try
        {
            while (!Thread.currentThread().isInterrupted())
            {
                PixelsPollingBatchFetcher.BucketBatch batch =
                        PixelsPollingBatchFetcher.pollBucket(sourceOptions, state.bucketId);
                if (!batch.isEmpty())
                {
                    emittedBatches.put(new EmittedBatch(batch.getBucketId(), batch.getRows()));
                }
                if (oneShot)
                {
                    return;
                }
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        catch (Exception e)
        {
            throw new RuntimeException("bucket polling failed bucket=" + state.bucketId, e);
        }
        finally
        {
            state.stopped = true;
        }
    }

    private static boolean allWorkersStopped(WorkerState[] states)
    {
        for (WorkerState state : states)
        {
            if (!state.stopped)
            {
                return false;
            }
        }
        return true;
    }

    private static Map<String, String> toSourceOptions(PixelsDeltaMergeOptions options)
    {
        Map<String, String> values = new LinkedHashMap<>();
        values.put(PixelsSourceOptions.HOST, options.getRpcHost());
        values.put(PixelsSourceOptions.PORT, String.valueOf(options.getRpcPort()));
        values.put(PixelsSourceOptions.DATABASE, options.getDatabase());
        values.put(PixelsSourceOptions.TABLE, options.getTable());
        values.put(PixelsSourceOptions.BENCHMARK, options.getBenchmark());
        values.put(PixelsSourceOptions.BUCKETS, joinBuckets(options.getBuckets()));
        values.put(PixelsSourceOptions.METADATA_HOST, options.getMetadataHost());
        values.put(PixelsSourceOptions.METADATA_PORT, String.valueOf(options.getMetadataPort()));
        return values;
    }

    private static String joinBuckets(List<Integer> buckets)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < buckets.size(); i++)
        {
            if (i > 0)
            {
                builder.append(',');
            }
            builder.append(buckets.get(i));
        }
        return builder.toString();
    }

    private static boolean isOneShot(PixelsDeltaMergeOptions options)
    {
        return "once".equalsIgnoreCase(options.getTriggerMode())
                || "available-now".equalsIgnoreCase(options.getTriggerMode());
    }

    private static final class EmittedBatch
    {
        private final int bucketId;
        private final List<Row> rows;

        private EmittedBatch(int bucketId, List<Row> rows)
        {
            this.bucketId = bucketId;
            this.rows = rows;
        }
    }

    private static final class WorkerState
    {
        private final int bucketId;
        private volatile boolean stopped;

        private WorkerState(int bucketId)
        {
            this.bucketId = bucketId;
            this.stopped = false;
        }
    }
}
