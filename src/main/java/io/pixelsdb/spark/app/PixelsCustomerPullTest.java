package io.pixelsdb.spark.app;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.spark.rpc.PixelsRpcClient;

import java.util.Arrays;
import java.util.List;

public class PixelsCustomerPullTest
{
    public static void main(String[] args)
    {
        String host = args.length > 0 ? args[0] : "realtime-pixels-coordinator";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9091;
        String database = args.length > 2 ? args[2] : "pixels_bench_sf10x";
        String table = args.length > 3 ? args[3] : "customer";
        List<Integer> buckets = args.length > 4
                ? Arrays.asList(Integer.parseInt(args[4]))
                : Arrays.asList(0);

        try (PixelsRpcClient client = new PixelsRpcClient(host, port))
        {
            List<SinkProto.RowRecord> records = client.pollEvents(database, table, buckets);
            System.out.println("host=" + host);
            System.out.println("port=" + port);
            System.out.println("database=" + database);
            System.out.println("table=" + table);
            System.out.println("buckets=" + buckets);
            System.out.println("record_count=" + records.size());

            int limit = Math.min(records.size(), 5);
            for (int i = 0; i < limit; i++)
            {
                SinkProto.RowRecord record = records.get(i);
                System.out.println("record[" + i + "].op=" + record.getOp());
                System.out.println("record[" + i + "]=" + record);
            }
        }
    }
}
