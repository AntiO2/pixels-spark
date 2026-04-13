package io.pixelsdb.spark.source;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.spark.rpc.PixelsRpcClient;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class PixelsPollingBatchFetcher
{
    private static final Logger LOG = LoggerFactory.getLogger(PixelsPollingBatchFetcher.class);

    public static final class BucketBatch
    {
        private final int bucketId;
        private final List<Row> rows;

        public BucketBatch(int bucketId, List<Row> rows)
        {
            this.bucketId = bucketId;
            this.rows = rows;
        }

        public int getBucketId()
        {
            return bucketId;
        }

        public List<Row> getRows()
        {
            return rows;
        }

        public boolean isEmpty()
        {
            return rows.isEmpty();
        }

        public int size()
        {
            return rows.size();
        }
    }

    private PixelsPollingBatchFetcher()
    {
    }

    public static List<Integer> buckets(PixelsSourceOptions sourceOptions)
    {
        if (sourceOptions.getBuckets().isEmpty())
        {
            return java.util.Collections.singletonList(0);
        }
        return sourceOptions.getBuckets();
    }

    public static BucketBatch pollBucket(PixelsSourceOptions sourceOptions, int bucketId)
    {
        StructType schema = PixelsMetadataSchemaLoader.load(sourceOptions);
        List<Row> rows = fetchBucketRows(schema, sourceOptions, bucketId);
        return new BucketBatch(bucketId, rows);
    }

    public static Dataset<Row> toDataset(SparkSession spark, PixelsSourceOptions sourceOptions, List<Row> rows)
    {
        StructType schema = PixelsMetadataSchemaLoader.load(sourceOptions);
        JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
        return spark.createDataFrame(context.parallelize(rows, 1), schema);
    }

    private static List<Row> fetchBucketRows(StructType schema, PixelsSourceOptions sourceOptions, int bucketId)
    {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(PixelsSourceOptions.HOST, sourceOptions.getHost());
        options.put(PixelsSourceOptions.PORT, String.valueOf(sourceOptions.getPort()));
        options.put(PixelsSourceOptions.DATABASE, sourceOptions.getDatabase());
        options.put(PixelsSourceOptions.TABLE, sourceOptions.getTable());
        options.put(PixelsSourceOptions.BUCKETS, String.valueOf(bucketId));
        if (sourceOptions.getMetadataHost() != null)
        {
            options.put(PixelsSourceOptions.METADATA_HOST, sourceOptions.getMetadataHost());
        }
        if (sourceOptions.getMetadataPort() != null)
        {
            options.put(PixelsSourceOptions.METADATA_PORT, String.valueOf(sourceOptions.getMetadataPort()));
        }
        options.put(PixelsSourceOptions.MAX_ROWS_PER_BATCH, String.valueOf(sourceOptions.getMaxRowsPerBatch()));
        options.put(PixelsSourceOptions.MAX_WAIT_MS_PER_BATCH, String.valueOf(sourceOptions.getMaxWaitMsPerBatch()));
        options.put(PixelsSourceOptions.REFRESH_MAX_WAIT_ON_NON_EMPTY_POLL,
                String.valueOf(sourceOptions.shouldRefreshMaxWaitOnNonEmptyPoll()));
        options.put(PixelsSourceOptions.EMPTY_POLL_SLEEP_MS, String.valueOf(sourceOptions.getEmptyPollSleepMs()));

        PixelsSourceOptions bucketOptions = new PixelsSourceOptions(options);
        PixelsRowRecordDecoder decoder = new PixelsRowRecordDecoder(schema, bucketId);
        List<Row> rows = new ArrayList<>();
        long maxWaitMs = Math.max(0L, bucketOptions.getMaxWaitMsPerBatch());
        long deadline = System.currentTimeMillis() + maxWaitMs;

        try (PixelsRpcClient client = new PixelsRpcClient(bucketOptions.getHost(), bucketOptions.getPort()))
        {
            while (rows.size() < bucketOptions.getMaxRowsPerBatch())
            {
                List<SinkProto.RowRecord> polled = client.pollEvents(
                        bucketOptions.getDatabase(),
                        bucketOptions.getTable(),
                        bucketOptions.getBuckets());
                if (!polled.isEmpty())
                {
                    for (SinkProto.RowRecord rowRecord : polled)
                    {
                        Row row = decoder.decodeExternal(rowRecord);
                        if (row != null)
                        {
                            rows.add(row);
                        }
                    }
                    if (bucketOptions.shouldRefreshMaxWaitOnNonEmptyPoll())
                    {
                        deadline = System.currentTimeMillis() + maxWaitMs;
                    }
                    if (rows.size() >= bucketOptions.getMaxRowsPerBatch())
                    {
                        break;
                    }
                    continue;
                }

                if (System.currentTimeMillis() >= deadline)
                {
                    break;
                }

                long sleepMs = Math.max(0L, bucketOptions.getEmptyPollSleepMs());
                if (sleepMs > 0L)
                {
                    try
                    {
                        Thread.sleep(sleepMs);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        LOG.info("pollingFetch table={}.{} bucket={} rows={}",
                bucketOptions.getDatabase(), bucketOptions.getTable(), bucketId, rows.size());
        return rows;
    }
}
