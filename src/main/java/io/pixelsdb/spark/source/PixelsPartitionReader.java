package io.pixelsdb.spark.source;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.spark.rpc.PixelsRpcClient;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class PixelsPartitionReader implements PartitionReader<InternalRow>
{
    private final PixelsRpcClient client;
    private final PixelsRowRecordDecoder decoder;
    private final Iterator<SinkProto.RowRecord> iterator;
    private InternalRow current;

    public PixelsPartitionReader(PixelsInputPartition partition)
    {
        PixelsSourceOptions options = new PixelsSourceOptions(partition.getOptions());
        this.client = new PixelsRpcClient(options.getHost(), options.getPort());
        this.decoder = new PixelsRowRecordDecoder(partition.getSchema());
        this.iterator = fetchOnce(options).iterator();
    }

    private List<SinkProto.RowRecord> fetchOnce(PixelsSourceOptions options)
    {
        try
        {
            return client.pollEvents(options.getDatabase(), options.getTable(), options.getBuckets());
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
