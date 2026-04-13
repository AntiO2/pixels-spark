package io.pixelsdb.spark.merge;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

final class PixelsDeltaDeleteHandler implements PixelsDeltaOperationHandler
{
    @Override
    public boolean canHandle(Dataset<Row> rows, PixelsDeltaMergeContext context)
    {
        return !rows.isEmpty();
    }

    @Override
    public void handle(Dataset<Row> rows, PixelsDeltaMergeContext context)
    {
        if (PixelsDeltaMergeSupport.isHardDelete(context.getOptions()))
        {
            DeltaTable.forPath(context.getSpark(), context.getOptions().getTargetPath())
                    .as("t")
                    .merge(rows.as("s"), context.getMergeCondition())
                    .whenMatched()
                    .delete()
                    .execute();
            return;
        }

        if (PixelsDeltaMergeSupport.isSoftDelete(context.getOptions()))
        {
            DeltaTable.forPath(context.getSpark(), context.getOptions().getTargetPath())
                    .as("t")
                    .merge(rows.as("s"), context.getMergeCondition())
                    .whenMatched()
                    .updateExpr(PixelsDeltaMergeSupport.buildSoftDeleteAssignmentMap(
                            context.getTargetSchema(),
                            context.getOptions()))
                    .execute();
        }
    }
}
