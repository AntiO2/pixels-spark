package io.pixelsdb.spark.app;

import io.delta.tables.DeltaTable;
import io.pixelsdb.spark.config.PixelsSparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class PixelsDeltaOptimizeApp
{
    public static void main(String[] args)
    {
        if (args.length < 2)
        {
            throw new IllegalArgumentException(
                    "Usage: PixelsDeltaOptimizeApp <delta-path> <zorder-cols-csv> [spark-master] [compact-before-zorder] [checkpoint-after-optimize]");
        }

        String deltaPath = args[0];
        List<String> zOrderColumns = parseColumns(args[1]);
        String sparkMaster = args.length > 2 ? args[2] : PixelsSparkConfig.instance().get(PixelsSparkConfig.SPARK_MASTER);
        boolean compactBeforeZOrder = args.length > 3 && parseBooleanFlag(args[3]);
        boolean checkpointAfterOptimize = args.length > 4 && parseBooleanFlag(args[4]);

        SparkSession.Builder builder = SparkSession.builder()
                .appName("pixels-delta-optimize")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");

        if (sparkMaster != null && !sparkMaster.trim().isEmpty())
        {
            builder.master(sparkMaster.trim());
        }

        SparkSession spark = builder.getOrCreate();
        try
        {
            String sql = buildOptimizeSql(deltaPath, zOrderColumns);
            DeltaTable deltaTable = DeltaTable.forPath(spark, deltaPath);
            System.out.println("delta_path=" + deltaPath);
            System.out.println("zorder_columns=" + zOrderColumns);
            System.out.println("compact_before_zorder=" + compactBeforeZOrder);
            System.out.println("checkpoint_after_optimize=" + checkpointAfterOptimize);
            if (compactBeforeZOrder)
            {
                System.out.println("compaction_sql=OPTIMIZE delta.`" + deltaPath + "`");
                printRows("compaction_result", deltaTable.optimize().executeCompaction());
            }
            System.out.println("zorder_sql=" + sql);
            printRows("zorder_result", deltaTable.optimize().executeZOrderBy(zOrderColumns.toArray(new String[0])));
            if (checkpointAfterOptimize)
            {
                deltaTable.deltaLog().checkpoint();
                System.out.println("checkpoint_result=written");
            }
        }
        finally
        {
            spark.stop();
        }
    }

    private static String buildOptimizeSql(String deltaPath, List<String> zOrderColumns)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("OPTIMIZE delta.`").append(deltaPath).append("`");
        if (!zOrderColumns.isEmpty())
        {
            builder.append(" ZORDER BY (");
            for (int i = 0; i < zOrderColumns.size(); i++)
            {
                if (i > 0)
                {
                    builder.append(", ");
                }
                builder.append(zOrderColumns.get(i));
            }
            builder.append(')');
        }
        return builder.toString();
    }

    private static void printRows(String label, Dataset<Row> rows)
    {
        for (Row row : rows.collectAsList())
        {
            System.out.println(label + "=" + row);
        }
    }

    private static boolean parseBooleanFlag(String raw)
    {
        String normalized = raw == null ? "" : raw.trim().toLowerCase();
        switch (normalized)
        {
            case "1":
            case "true":
            case "yes":
            case "y":
            case "on":
                return true;
            case "0":
            case "false":
            case "no":
            case "n":
            case "off":
            case "":
                return false;
            default:
                throw new IllegalArgumentException("Invalid boolean flag: " + raw);
        }
    }

    private static List<String> parseColumns(String raw)
    {
        List<String> result = new ArrayList<>();
        for (String token : raw.split(","))
        {
            String trimmed = token.trim();
            if (trimmed.isEmpty())
            {
                continue;
            }
            if (!trimmed.matches("[A-Za-z_][A-Za-z0-9_]*"))
            {
                throw new IllegalArgumentException("Invalid Z-Order column: " + trimmed);
            }
            result.add(trimmed);
        }
        if (result.isEmpty())
        {
            throw new IllegalArgumentException("At least one Z-Order column is required");
        }
        return result;
    }
}
