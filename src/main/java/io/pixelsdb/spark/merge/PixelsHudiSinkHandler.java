package io.pixelsdb.spark.merge;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

final class PixelsHudiSinkHandler implements PixelsMergeSinkHandler
{
    @Override
    public void handle(PixelsDeltaPreparedBatch preparedBatch, PixelsDeltaMergeContext context, Long batchId)
    {
        Dataset<Row> inserts = PixelsHudiMergeSupport.prepareRowsForMerge(
                preparedBatch.getInserts(),
                context.getTargetSchema(),
                context.getPrimaryKeys());
        Dataset<Row> updates = PixelsHudiMergeSupport.prepareRowsForMerge(
                preparedBatch.getUpdates(),
                context.getTargetSchema(),
                context.getPrimaryKeys());
        Dataset<Row> deletes = PixelsHudiMergeSupport.prepareRowsForMerge(
                preparedBatch.getDeletes(),
                context.getTargetSchema(),
                context.getPrimaryKeys());

        mergeInserts(inserts, context);
        mergeUpdates(updates, context);
        mergeDeletes(deletes, context);
    }

    private static void mergeInserts(Dataset<Row> inserts, PixelsDeltaMergeContext context)
    {
        if (inserts.isEmpty())
        {
            return;
        }

        withTempView(inserts, "pixels_hudi_inserts", view -> {
            String sql = "MERGE INTO " + PixelsHudiMergeSupport.targetTableIdentifier(context.getOptions().getTargetPath())
                    + " t USING " + view + " s ON " + context.getMergeCondition()
                    + " WHEN NOT MATCHED THEN INSERT ("
                    + PixelsHudiMergeSupport.insertColumnsSql(context.getTargetSchema())
                    + ") VALUES ("
                    + PixelsHudiMergeSupport.insertValuesSql(context.getTargetSchema())
                    + ")";
            context.getSpark().sql(sql);
        });
    }

    private static void mergeUpdates(Dataset<Row> updates, PixelsDeltaMergeContext context)
    {
        if (updates.isEmpty())
        {
            return;
        }

        Map<String, String> assignments = PixelsHudiMergeSupport.buildUpsertAssignmentMap(context.getTargetSchema());
        withTempView(updates, "pixels_hudi_updates", view -> {
            String sql = "MERGE INTO " + PixelsHudiMergeSupport.targetTableIdentifier(context.getOptions().getTargetPath())
                    + " t USING " + view + " s ON " + context.getMergeCondition()
                    + " WHEN MATCHED THEN UPDATE SET "
                    + PixelsHudiMergeSupport.assignmentSql(assignments);
            context.getSpark().sql(sql);
        });
    }

    private static void mergeDeletes(Dataset<Row> deletes, PixelsDeltaMergeContext context)
    {
        if (deletes.isEmpty())
        {
            return;
        }

        withTempView(deletes, "pixels_hudi_deletes", view -> {
            if (PixelsMergeSupport.isHardDelete(context.getOptions()))
            {
                String sql = "MERGE INTO "
                        + PixelsHudiMergeSupport.targetTableIdentifier(context.getOptions().getTargetPath())
                        + " t USING " + view + " s ON " + context.getMergeCondition()
                        + " WHEN MATCHED THEN DELETE";
                context.getSpark().sql(sql);
                return;
            }

            if (PixelsMergeSupport.isSoftDelete(context.getOptions()))
            {
                String sql = "MERGE INTO "
                        + PixelsHudiMergeSupport.targetTableIdentifier(context.getOptions().getTargetPath())
                        + " t USING " + view + " s ON " + context.getMergeCondition()
                        + " WHEN MATCHED THEN UPDATE SET "
                        + PixelsHudiMergeSupport.assignmentSql(
                                PixelsHudiMergeSupport.buildSoftDeleteAssignmentMap(context.getTargetSchema()));
                context.getSpark().sql(sql);
            }
        });
    }

    private static void withTempView(Dataset<Row> rows, String prefix, SqlAction action)
    {
        String viewName = PixelsHudiMergeSupport.tempViewName(prefix);
        rows.createOrReplaceTempView(viewName);
        try
        {
            action.run(viewName);
        }
        finally
        {
            rows.sparkSession().catalog().dropTempView(viewName);
        }
    }

    @FunctionalInterface
    private interface SqlAction
    {
        void run(String tempViewName);
    }
}
