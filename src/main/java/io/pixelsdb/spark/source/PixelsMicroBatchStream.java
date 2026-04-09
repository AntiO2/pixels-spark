package io.pixelsdb.spark.source;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PixelsMicroBatchStream implements MicroBatchStream
{
    private static final Logger LOG = LoggerFactory.getLogger(PixelsMicroBatchStream.class);
    private final StructType schema;
    private final Map<String, String> options;
    private final AtomicLong nextOffset = new AtomicLong(0L);

    public PixelsMicroBatchStream(StructType schema, Map<String, String> options)
    {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public Offset latestOffset()
    {
        return new PixelsOffset(nextOffset.incrementAndGet());
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end)
    {
        long startValue = toLong(start);
        long endValue = toLong(end);
        if (endValue <= startValue)
        {
            return new InputPartition[0];
        }
        PixelsSourceOptions sourceOptions = new PixelsSourceOptions(options);
        if (sourceOptions.getBuckets().isEmpty())
        {
            LOG.info("planInputPartitions table={}.{} start={} end={} partitions=1 mode=unbucketed",
                    sourceOptions.getDatabase(), sourceOptions.getTable(), startValue, endValue);
            return new InputPartition[] {
                    new PixelsInputPartition(schema, options, endValue)
            };
        }

        InputPartition[] partitions = new InputPartition[sourceOptions.getBuckets().size()];
        for (int i = 0; i < sourceOptions.getBuckets().size(); i++)
        {
            Integer bucketId = sourceOptions.getBuckets().get(i);
            partitions[i] = new PixelsInputPartition(schema, options, endValue, bucketId);
        }
        LOG.info("planInputPartitions table={}.{} start={} end={} partitions={} buckets={}",
                sourceOptions.getDatabase(),
                sourceOptions.getTable(),
                startValue,
                endValue,
                partitions.length,
                sourceOptions.getBuckets());
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        return new PixelsPartitionReaderFactory();
    }

    @Override
    public Offset initialOffset()
    {
        return new PixelsOffset(0L);
    }

    @Override
    public Offset deserializeOffset(String json)
    {
        return new PixelsOffset(Long.parseLong(json));
    }

    @Override
    public void commit(Offset end)
    {
    }

    @Override
    public void stop()
    {
    }

    private long toLong(Offset offset)
    {
        if (offset == null)
        {
            return 0L;
        }
        return ((PixelsOffset) offset).getValue();
    }
}
