package io.pixelsdb.spark.rpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.DnsNameResolverProvider;
import io.pixelsdb.pixels.sink.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.SinkProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PixelsRpcClient implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(PixelsRpcClient.class);

    private final ManagedChannel channel;
    private final PixelsPollingServiceGrpc.PixelsPollingServiceBlockingStub blockingStub;

    public PixelsRpcClient(String host, int port)
    {
        this(ManagedChannelBuilder.forAddress(host, port)
                .nameResolverFactory(new DnsNameResolverProvider())
                .usePlaintext()
                .build());
    }

    PixelsRpcClient(ManagedChannel channel)
    {
        this.channel = channel;
        this.blockingStub = PixelsPollingServiceGrpc.newBlockingStub(channel);
    }

    public List<SinkProto.RowRecord> pollEvents(
            String schemaName,
            String tableName,
            List<Integer> buckets)
    {
        SinkProto.PollRequest request = SinkProto.PollRequest.newBuilder()
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .addAllBuckets(buckets)
                .build();
        long startedAt = System.currentTimeMillis();
        try
        {
            SinkProto.PollResponse pollResponse = blockingStub.pollEvents(request);
            List<SinkProto.RowRecord> records = pollResponse.getRecordsList();
            long elapsedMs = System.currentTimeMillis() - startedAt;
            LOG.info("pollEvents schema={} table={} buckets={} rows={} elapsedMs={}",
                    schemaName, tableName, buckets, records.size(), elapsedMs);
            return records;
        }
        catch (Exception e)
        {
            long elapsedMs = System.currentTimeMillis() - startedAt;
            LOG.error("RPC call failed schema={} table={} buckets={} elapsedMs={} error={}",
                    schemaName, tableName, buckets, elapsedMs, e.getMessage());
            throw e;
        }
    }

    @Override
    public void close()
    {
        try
        {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            LOG.warn("Interrupted during shutdown", e);
            Thread.currentThread().interrupt();
        }
    }
}
