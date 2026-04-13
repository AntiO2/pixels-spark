package io.pixelsdb.spark.merge;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

final class PixelsDeltaUpdateHandler implements PixelsDeltaOperationHandler
{
    @Override
    public boolean canHandle(Dataset<Row> rows, PixelsDeltaMergeContext context)
    {
        return !rows.isEmpty();
    }

    @Override
    public void handle(Dataset<Row> rows, PixelsDeltaMergeContext context)
    {
        DeltaTable.forPath(context.getSpark(), context.getOptions().getTargetPath())
                .as("t")
                .merge(rows.as("s"), context.getMergeCondition())
                .whenMatched()
                .updateExpr(PixelsDeltaMergeSupport.buildUpsertAssignmentMap(
                        context.getTargetSchema(),
                        context.getOptions()))
                .execute();
    }
}
