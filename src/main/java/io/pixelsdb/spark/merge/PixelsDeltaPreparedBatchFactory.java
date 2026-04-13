package io.pixelsdb.spark.merge;

import io.pixelsdb.spark.source.PixelsMetadataColumns;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

final class PixelsDeltaPreparedBatchFactory
{
    private PixelsDeltaPreparedBatchFactory()
    {
    }

    static PixelsDeltaPreparedBatch prepare(
            Dataset<Row> batch,
            List<String> primaryKeys,
            PixelsDeltaMergeOptions options)
    {
        Dataset<Row> latestRows = options.isEnableLatestPerPrimaryKey()
                ? latestPerPrimaryKey(batch, primaryKeys).cache()
                : batch.cache();

        Dataset<Row> inserts = businessColumnsOnly(latestRows.filter(
                col(PixelsMetadataColumns.OP).equalTo("INSERT")
                        .or(col(PixelsMetadataColumns.OP).equalTo("SNAPSHOT"))));
        Dataset<Row> updates = businessColumnsOnly(latestRows.filter(col(PixelsMetadataColumns.OP).equalTo("UPDATE")));
        Dataset<Row> deletes = businessColumnsOnly(latestRows.filter(col(PixelsMetadataColumns.OP).equalTo("DELETE")));
        return new PixelsDeltaPreparedBatch(latestRows, inserts, updates, deletes);
    }

    static Dataset<Row> prepareInsertRows(
            Dataset<Row> inserts,
            StructType targetSchema)
    {
        List<String> expressions = new ArrayList<>();
        for (StructField field : targetSchema.fields())
        {
            if (PixelsDeltaMergeColumns.IS_DELETED.equals(field.name()))
            {
                expressions.add("false AS " + field.name());
            }
            else if (PixelsDeltaMergeColumns.DELETED_AT.equals(field.name()))
            {
                expressions.add("CAST(NULL AS TIMESTAMP) AS " + field.name());
            }
            else
            {
                expressions.add(field.name());
            }
        }
        return inserts.selectExpr(expressions.toArray(new String[0]));
    }

    private static Dataset<Row> latestPerPrimaryKey(Dataset<Row> batch, List<String> primaryKeys)
    {
        List<Column> partitions = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys)
        {
            partitions.add(col(primaryKey));
        }

        WindowSpec window = Window.partitionBy(partitions.toArray(new Column[0]))
                .orderBy(
                        col(PixelsMetadataColumns.TOTAL_ORDER).desc_nulls_last(),
                        col(PixelsMetadataColumns.DATA_COLLECTION_ORDER).desc_nulls_last(),
                        col(PixelsMetadataColumns.TXN_ID).desc_nulls_last());

        return batch.withColumn("_pixels_rank", row_number().over(window))
                .filter(col("_pixels_rank").equalTo(1))
                .drop("_pixels_rank");
    }

    private static Dataset<Row> businessColumnsOnly(Dataset<Row> batch)
    {
        List<String> columns = new ArrayList<>();
        for (StructField field : batch.schema().fields())
        {
            if (!PixelsMetadataColumns.isMetadataColumn(field.name()))
            {
                columns.add(field.name());
            }
        }
        return batch.selectExpr(columns.toArray(new String[0]));
    }
}
