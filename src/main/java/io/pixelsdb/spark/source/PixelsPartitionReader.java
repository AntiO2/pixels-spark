package io.pixelsdb.spark.source;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.spark.rpc.PixelsRpcClient;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PixelsPartitionReader implements PartitionReader<InternalRow>
{
    private static final Logger LOG = LoggerFactory.getLogger(PixelsPartitionReader.class);
    private final PixelsRpcClient client;
    private final PixelsRowRecordDecoder decoder;
    private final Iterator<SinkProto.RowRecord> iterator;
    private InternalRow current;

    public PixelsPartitionReader(PixelsInputPartition partition)
    {
        PixelsSourceOptions options = new PixelsSourceOptions(partition.getOptions());
        this.client = new PixelsRpcClient(options.getHost(), options.getPort());
        this.decoder = new PixelsRowRecordDecoder(partition.getSchema(), requireBucketId(partition));
        this.iterator = fetchBatch(options).iterator();
    }

    private static int requireBucketId(PixelsInputPartition partition)
    {
        if (partition.getBucketId() == null)
        {
            throw new IllegalArgumentException("Pixels CDC source requires a bucketId per input partition");
        }
        return partition.getBucketId();
    }

    private List<SinkProto.RowRecord> fetchBatch(PixelsSourceOptions options)
    {
        List<SinkProto.RowRecord> records = new ArrayList<>();
        long maxWaitMs = Math.max(0L, options.getMaxWaitMsPerBatch());
        long deadline = System.currentTimeMillis() + maxWaitMs;
        try
        {
            while (records.size() < options.getMaxRowsPerBatch())
            {
                List<SinkProto.RowRecord> polled =
                        client.pollEvents(options.getDatabase(), options.getTable(), options.getBuckets());
                if (!polled.isEmpty())
                {
                    records.addAll(polled);
                    if (options.shouldRefreshMaxWaitOnNonEmptyPoll())
                    {
                        deadline = System.currentTimeMillis() + maxWaitMs;
                    }

                    if (records.size() >= options.getMaxRowsPerBatch())
                    {
                        break;
                    }
                    continue;
                }

                if (System.currentTimeMillis() >= deadline)
                {
                    break;
                }

                long sleepMs = Math.max(0L, options.getEmptyPollSleepMs());
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
            LOG.info("fetchBatch table={}.{} buckets={} rows={}",
                    options.getDatabase(), options.getTable(), options.getBuckets(), records.size());
            return records;
        }
        catch (RuntimeException e)
        {
            closeQuietly();
            throw e;
        }
    }

    @Override
    public boolean next()
    {
        while (iterator.hasNext())
        {
            InternalRow row = decoder.decode(iterator.next());
            if (row != null)
            {
                current = row;
                return true;
            }
        }
        current = null;
        return false;
    }

    @Override
    public InternalRow get()
    {
        return current;
    }

    @Override
    public void close() throws IOException
    {
        client.close();
    }

    private void closeQuietly()
    {
        try
        {
            close();
        }
        catch (IOException ignored)
        {
        }
    }
}
