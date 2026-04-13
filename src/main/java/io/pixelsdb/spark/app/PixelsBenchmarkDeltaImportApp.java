package io.pixelsdb.spark.app;

import io.pixelsdb.spark.benchmark.BenchmarkTableDefinition;
import io.pixelsdb.spark.benchmark.BenchmarkTableRegistry;
import io.pixelsdb.spark.bucket.PixelsBucketId;
import io.pixelsdb.spark.config.PixelsSparkConfig;
import io.pixelsdb.spark.merge.PixelsDeltaMergeColumns;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.col;

public class PixelsBenchmarkDeltaImportApp
{
    private static final String DELTA_ENABLE_DELETION_VECTORS_PROPERTY = "delta.enableDeletionVectors";
    private static final String IMPORT_CHUNK_TEMP_DIR = "pixels.import.chunk-temp-dir";
    private static final String IMPORT_BENCHMARK = "pixels.import.benchmark";

    public static void main(String[] args)
    {
        String csvRoot = args.length > 0 ? args[0] : "/home/antio2/projects/pixels-benchmark/Data_1x";
        String deltaRoot = args.length > 1 ? args[1]
                : PixelsSparkConfig.instance().getOrDefault("pixels.import.local.delta-root",
                "/home/ubuntu/disk1/tmp/pixels-benchmark-deltalake/data_1x");
        String sparkMaster = args.length > 2 ? args[2] : PixelsSparkConfig.instance().get(PixelsSparkConfig.SPARK_MASTER);
        String selectedTablesArg = null;
        String benchmarkArg = null;
        if (args.length > 4)
        {
            selectedTablesArg = args[3];
            benchmarkArg = args[4];
        }
        else if (args.length > 3)
        {
            String arg3 = args[3];
            if (BenchmarkTableRegistry.BENCHMARK_HYBENCH.equalsIgnoreCase(arg3)
                    || BenchmarkTableRegistry.BENCHMARK_CHBENCHMARK.equalsIgnoreCase(arg3))
            {
                benchmarkArg = arg3;
            }
            else
            {
                selectedTablesArg = arg3;
            }
        }

        Set<String> selectedTables = parseSelectedTables(selectedTablesArg);
        String benchmark = BenchmarkTableRegistry.normalizeBenchmark(benchmarkArg != null ? benchmarkArg
                : PixelsSparkConfig.instance().getOrDefault(
                IMPORT_BENCHMARK,
                BenchmarkTableRegistry.BENCHMARK_HYBENCH));
        int chunkRows = PixelsSparkConfig.instance()
                .getIntOrDefault(PixelsSparkConfig.IMPORT_CSV_CHUNK_ROWS, 2560000);
        boolean countRows = PixelsSparkConfig.instance()
                .getBooleanOrDefault(PixelsSparkConfig.IMPORT_COUNT_ROWS, false);
        int bucketCount = PixelsSparkConfig.instance().getIntOrDefault("node.bucket.num", 0);
        boolean enableDeletionVectors = PixelsSparkConfig.instance()
                .getBooleanOrDefault(PixelsSparkConfig.DELTA_ENABLE_DELETION_VECTORS, true);

        List<BenchmarkTableDefinition> tableDefinitions = BenchmarkTableRegistry.list(benchmark);

        SparkSession.Builder builder = SparkSession.builder()
                .appName("pixels-benchmark-delta-import")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");

        if (sparkMaster != null && !sparkMaster.trim().isEmpty())
        {
            builder.master(sparkMaster.trim());
        }

        SparkSession spark = builder.getOrCreate();
        try
        {
            for (BenchmarkTableDefinition definition : tableDefinitions)
            {
                if (selectedTables != null && !selectedTables.contains(definition.getTableName()))
                {
                    continue;
                }
                String csvPath = csvRoot + "/" + definition.getFileName();
                String deltaPath = deltaRoot + "/" + definition.getTableName();
                long rowCount = importTableInChunks(
                        spark,
                        definition,
                        csvPath,
                        deltaPath,
                        bucketCount,
                        enableDeletionVectors,
                        chunkRows,
                        countRows);

                System.out.println("table=" + definition.getTableName()
                        + " csv_path=" + csvPath
                        + " delta_path=" + deltaPath
                        + " benchmark=" + benchmark
                        + " bucket_count=" + bucketCount
                        + " deletion_vectors=" + enableDeletionVectors
                        + " chunk_rows=" + chunkRows
                        + (countRows ? " row_count=" + rowCount : ""));
            }
        }
        finally
        {
            spark.stop();
        }
    }

    private static Set<String> parseSelectedTables(String tablesArg)
    {
        if (tablesArg == null || tablesArg.trim().isEmpty())
        {
            return null;
        }

        Set<String> selectedTables = new HashSet<>();
        for (String table : tablesArg.split(","))
        {
            String normalized = table.trim();
            if (!normalized.isEmpty())
            {
                selectedTables.add(normalized);
            }
        }
        return selectedTables.isEmpty() ? null : selectedTables;
    }

    private static long importTableInChunks(
            SparkSession spark,
            BenchmarkTableDefinition definition,
            String csvPath,
            String deltaPath,
            int bucketCount,
            boolean enableDeletionVectors,
            int chunkRows,
            boolean countRows)
    {
        if (chunkRows <= 0)
        {
            Dataset<Row> dataset = spark.read()
                    .format("csv")
                    .schema(definition.getSchema())
                    .option("header", "false")
                    .option("mode", "FAILFAST")
                    .option("nullValue", "")
                    .option("delimiter", definition.getDelimiter())
                    .load(csvPath);
            Dataset<Row> output = withHashBucketColumn(dataset, definition, bucketCount);
            if (bucketCount > 0)
            {
                output = output.repartition(bucketCount, col(PixelsDeltaMergeColumns.BUCKET_ID));
            }
            writeChunk(output, deltaPath, bucketCount, enableDeletionVectors, true);
            spark.sql("ALTER TABLE delta.`" + deltaPath + "` SET TBLPROPERTIES ("
                    + "'" + DELTA_ENABLE_DELETION_VECTORS_PROPERTY + "'='"
                    + String.valueOf(enableDeletionVectors) + "')");
            return countRows ? output.count() : -1L;
        }

        long totalRows = 0L;
        boolean firstChunk = true;
        Path tempDir;
        try
        {
            Path chunkTempRoot = Paths.get(PixelsSparkConfig.instance().getOrDefault(
                    IMPORT_CHUNK_TEMP_DIR,
                    "/home/ubuntu/disk1/tmp/pixels-chunks"));
            Files.createDirectories(chunkTempRoot);
            tempDir = Files.createTempDirectory(chunkTempRoot, "pixels-csv-chunks-");
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to create temp chunk directory", e);
        }

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(csvPath)))
        {
            while (true)
            {
                Path chunkPath = writeNextChunk(reader, tempDir, chunkRows);
                if (chunkPath == null)
                {
                    break;
                }

                Dataset<Row> chunk = spark.read()
                        .format("csv")
                        .schema(definition.getSchema())
                        .option("header", "false")
                        .option("mode", "FAILFAST")
                        .option("nullValue", "")
                        .option("delimiter", definition.getDelimiter())
                        .load(chunkPath.toString());

                Dataset<Row> output = withHashBucketColumn(chunk, definition, bucketCount);
                if (bucketCount > 0)
                {
                    output = output.repartition(bucketCount, col(PixelsDeltaMergeColumns.BUCKET_ID));
                }
                writeChunk(output, deltaPath, bucketCount, enableDeletionVectors, firstChunk);
                if (countRows)
                {
                    totalRows += output.count();
                }
                firstChunk = false;
                Files.deleteIfExists(chunkPath);
            }

            if (!firstChunk)
            {
                spark.sql("ALTER TABLE delta.`" + deltaPath + "` SET TBLPROPERTIES ("
                        + "'" + DELTA_ENABLE_DELETION_VECTORS_PROPERTY + "'='"
                        + String.valueOf(enableDeletionVectors) + "')");
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to import CSV in chunks from " + csvPath, e);
        }
        finally
        {
            try
            {
                Files.deleteIfExists(tempDir);
            }
            catch (IOException ignored)
            {
            }
        }

        return totalRows;
    }

    private static Path writeNextChunk(BufferedReader reader, Path tempDir, int chunkRows) throws IOException
    {
        Path chunkPath = Files.createTempFile(tempDir, "chunk-", ".csv");
        int linesWritten = 0;

        try (BufferedWriter writer = Files.newBufferedWriter(chunkPath))
        {
            String line;
            while (linesWritten < chunkRows && (line = reader.readLine()) != null)
            {
                writer.write(line);
                writer.newLine();
                linesWritten++;
            }
        }

        if (linesWritten == 0)
        {
            Files.deleteIfExists(chunkPath);
            return null;
        }
        return chunkPath;
    }

    private static void writeChunk(
            Dataset<Row> output,
            String deltaPath,
            int bucketCount,
            boolean enableDeletionVectors,
            boolean firstChunk)
    {
        org.apache.spark.sql.DataFrameWriter<Row> writer = output.write()
                .format("delta")
                .mode(firstChunk ? "overwrite" : "append")
                .option(DELTA_ENABLE_DELETION_VECTORS_PROPERTY, String.valueOf(enableDeletionVectors));

        if (bucketCount > 0)
        {
            writer.partitionBy(PixelsDeltaMergeColumns.BUCKET_ID);
        }

        writer.save(deltaPath);
    }

    private static Dataset<Row> withHashBucketColumn(
            Dataset<Row> dataset,
            BenchmarkTableDefinition definition,
            int bucketCount)
    {
        return PixelsBucketId.withBucketColumnFromSchema(
                dataset,
                definition.getPrimaryKeyColumns(),
                definition.getSchema(),
                bucketCount);
    }
}
