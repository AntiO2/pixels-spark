package io.pixelsdb.spark.merge;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

final class PixelsDeltaPreparedBatch
{
    private final Dataset<Row> latestRows;
    private final Dataset<Row> inserts;
    private final Dataset<Row> updates;
    private final Dataset<Row> deletes;

    PixelsDeltaPreparedBatch(
            Dataset<Row> latestRows,
            Dataset<Row> inserts,
            Dataset<Row> updates,
            Dataset<Row> deletes)
    {
        this.latestRows = latestRows;
        this.inserts = inserts;
        this.updates = updates;
        this.deletes = deletes;
    }

    Dataset<Row> getInserts()
    {
        return inserts;
    }

    Dataset<Row> getUpdates()
    {
        return updates;
    }

    Dataset<Row> getDeletes()
    {
        return deletes;
    }

    void close()
    {
        latestRows.unpersist();
    }
}
