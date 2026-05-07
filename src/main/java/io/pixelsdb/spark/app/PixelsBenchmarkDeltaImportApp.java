package io.pixelsdb.spark.app;

import io.pixelsdb.spark.benchmark.BenchmarkTableDefinition;
import io.pixelsdb.spark.benchmark.BenchmarkTableRegistry;
import io.pixelsdb.spark.bucket.PixelsBucketId;
import io.pixelsdb.spark.config.PixelsSparkConfig;
import io.pixelsdb.spark.hudi.PixelsHudiPartitioning;
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
    private static final String MODE_DELTA = "delta";
    private static final String MODE_HUDI = "hudi";
    private static final String HUDI_INDEX_SIMPLE = "SIMPLE";
    private static final String HUDI_INDEX_RECORD_LEVEL = "RECORD_LEVEL_INDEX";

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
        String importMode = normalizeImportMode(firstNonEmpty(
                System.getenv("IMPORT_MODE"),
                PixelsSparkConfig.instance().getOrDefault(PixelsSparkConfig.IMPORT_MODE, MODE_DELTA)));
        int bucketCount = PixelsSparkConfig.instance().getIntOrDefault("node.bucket.num", 0);
        int hudiPartitionCount = PixelsHudiPartitioning.resolvePartitionCount(bucketCount);
        boolean enableDeletionVectors = PixelsSparkConfig.instance()
                .getBooleanOrDefault(PixelsSparkConfig.DELTA_ENABLE_DELETION_VECTORS, true);

        List<BenchmarkTableDefinition> tableDefinitions = BenchmarkTableRegistry.list(benchmark);

        SparkSession.Builder builder = SparkSession.builder()
                .appName("pixels-benchmark-" + importMode + "-import");

        if (MODE_HUDI.equals(importMode))
        {
            builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog");
        }
        else
        {
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
        }

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
                        hudiPartitionCount,
                        enableDeletionVectors,
                        importMode,
                        chunkRows,
                        countRows);

                System.out.println("table=" + definition.getTableName()
                        + " csv_path=" + csvPath
                        + " delta_path=" + deltaPath
                        + " benchmark=" + benchmark
                        + " import_mode=" + importMode
                        + " bucket_count=" + bucketCount
                        + " hudi_partition_count=" + hudiPartitionCount
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
            int hudiPartitionCount,
            boolean enableDeletionVectors,
            String importMode,
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
            Dataset<Row> output = dataset;
                if (MODE_DELTA.equals(importMode))
                {
                    output = withHashBucketColumn(dataset, definition, bucketCount);
                    if (bucketCount > 0)
                    {
                        output = output.repartition(bucketCount, col(PixelsDeltaMergeColumns.BUCKET_ID));
                    }
                }
                else
                {
                    output = withHudiPartitionColumn(dataset, definition, hudiPartitionCount);
                    output = output.repartition(hudiPartitionCount, col(PixelsHudiPartitioning.HUDI_PARTITION_COLUMN));
                }
            writeChunk(output, definition, deltaPath, bucketCount, hudiPartitionCount, enableDeletionVectors, importMode, true);
            updateTablePropertiesAfterImport(spark, deltaPath, enableDeletionVectors, importMode);
            if (MODE_HUDI.equals(importMode) && shouldBuildRecordLevelIndex(definition))
            {
                enableHudiRecordLevelIndex(spark, definition, deltaPath, hudiPartitionCount);
            }
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

                Dataset<Row> output = chunk;
                if (MODE_DELTA.equals(importMode))
                {
                    output = withHashBucketColumn(chunk, definition, bucketCount);
                    if (bucketCount > 0)
                    {
                        output = output.repartition(bucketCount, col(PixelsDeltaMergeColumns.BUCKET_ID));
                    }
                }
                else
                {
                    output = withHudiPartitionColumn(chunk, definition, hudiPartitionCount);
                    output = output.repartition(hudiPartitionCount, col(PixelsHudiPartitioning.HUDI_PARTITION_COLUMN));
                }
                writeChunk(output, definition, deltaPath, bucketCount, hudiPartitionCount, enableDeletionVectors, importMode, firstChunk);
                if (countRows)
                {
                    totalRows += output.count();
                }
                firstChunk = false;
                Files.deleteIfExists(chunkPath);
            }

            if (!firstChunk)
            {
                updateTablePropertiesAfterImport(spark, deltaPath, enableDeletionVectors, importMode);
                if (MODE_HUDI.equals(importMode) && shouldBuildRecordLevelIndex(definition))
                {
                    enableHudiRecordLevelIndex(spark, definition, deltaPath, hudiPartitionCount);
                }
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
            BenchmarkTableDefinition definition,
            String targetPath,
            int bucketCount,
            int hudiPartitionCount,
            boolean enableDeletionVectors,
            String importMode,
            boolean firstChunk)
    {
        if (MODE_HUDI.equals(importMode))
        {
            writeHudiChunk(output, definition, targetPath, hudiPartitionCount, firstChunk);
            return;
        }
        writeDeltaChunk(output, targetPath, bucketCount, enableDeletionVectors, firstChunk);
    }

    private static void writeDeltaChunk(
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

    private static void writeHudiChunk(
            Dataset<Row> output,
            BenchmarkTableDefinition definition,
            String targetPath,
            int hudiPartitionCount,
            boolean firstChunk)
    {
        String recordKeyField = String.join(",", definition.getPrimaryKeyColumns());
        String precombineField = selectPrecombineField(definition);
        String operation = firstChunk ? "bulk_insert" : "upsert";

        org.apache.spark.sql.DataFrameWriter<Row> writer = output.write()
                .format("hudi")
                .mode(firstChunk ? "overwrite" : "append")
                .option("hoodie.table.name", definition.getTableName().toLowerCase())
                .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
                .option("hoodie.datasource.write.operation", operation)
                .option("hoodie.datasource.write.recordkey.field", recordKeyField)
                .option("hoodie.datasource.write.precombine.field", precombineField)
                .option("hoodie.datasource.write.partitionpath.field", PixelsHudiPartitioning.HUDI_PARTITION_COLUMN)
                .option("hoodie.insert.shuffle.parallelism", String.valueOf(Math.max(1, hudiPartitionCount)))
                .option("hoodie.upsert.shuffle.parallelism", String.valueOf(Math.max(1, hudiPartitionCount)))
                .option("hoodie.metadata.enable", "false")
                .option("hoodie.metadata.record.index.enable", "false")
                .option("hoodie.index.type", HUDI_INDEX_SIMPLE)
                .option("hoodie.datasource.write.keygenerator.class",
                        definition.getPrimaryKeyColumns().size() > 1
                                ? "org.apache.hudi.keygen.ComplexKeyGenerator"
                                : "org.apache.hudi.keygen.SimpleKeyGenerator");

        writer.save(targetPath);
    }

    private static void updateTablePropertiesAfterImport(
            SparkSession spark,
            String targetPath,
            boolean enableDeletionVectors,
            String importMode)
    {
        if (!MODE_DELTA.equals(importMode))
        {
            return;
        }
        spark.sql("ALTER TABLE delta.`" + targetPath + "` SET TBLPROPERTIES ("
                + "'" + DELTA_ENABLE_DELETION_VECTORS_PROPERTY + "'='"
                + String.valueOf(enableDeletionVectors) + "')");
    }

    private static void enableHudiRecordLevelIndex(
            SparkSession spark,
            BenchmarkTableDefinition definition,
            String targetPath,
            int hudiPartitionCount)
    {
        StructType targetSchema = PixelsHudiPartitioning.withPartitionColumnInSchema(definition.getSchema());
        String[] selectedColumns = new String[targetSchema.fields().length];
        for (int i = 0; i < targetSchema.fields().length; i++)
        {
            selectedColumns[i] = targetSchema.fields()[i].name();
        }

        Dataset<Row> sample = spark.read()
                .format("hudi")
                .load(targetPath)
                .selectExpr(selectedColumns)
                .limit(1);
        if (sample.isEmpty())
        {
            return;
        }

        String recordKeyField = String.join(",", definition.getPrimaryKeyColumns());
        String precombineField = selectPrecombineField(definition);
        sample.write()
                .format("hudi")
                .mode("append")
                .option("hoodie.table.name", definition.getTableName().toLowerCase())
                .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
                .option("hoodie.datasource.write.operation", "upsert")
                .option("hoodie.datasource.write.recordkey.field", recordKeyField)
                .option("hoodie.datasource.write.precombine.field", precombineField)
                .option("hoodie.datasource.write.partitionpath.field", PixelsHudiPartitioning.HUDI_PARTITION_COLUMN)
                .option("hoodie.datasource.write.keygenerator.class",
                        definition.getPrimaryKeyColumns().size() > 1
                                ? "org.apache.hudi.keygen.ComplexKeyGenerator"
                                : "org.apache.hudi.keygen.SimpleKeyGenerator")
                .option("hoodie.insert.shuffle.parallelism", "1")
                .option("hoodie.upsert.shuffle.parallelism", String.valueOf(Math.max(1, hudiPartitionCount)))
                .option("hoodie.metadata.enable", "true")
                .option("hoodie.metadata.record.level.index.enable", "true")
                .option("hoodie.metadata.record.index.enable", "false")
                .option("hoodie.metadata.global.record.level.index.enable", "false")
                .option("hoodie.index.type", HUDI_INDEX_RECORD_LEVEL)
                .save(targetPath);
    }

    private static boolean shouldBuildRecordLevelIndex(BenchmarkTableDefinition definition)
    {
        return !"transfer".equalsIgnoreCase(definition.getTableName());
    }

    private static String selectPrecombineField(BenchmarkTableDefinition definition)
    {
        if (definition.getSchema().getFieldIndex("freshness_ts").isDefined())
        {
            return "freshness_ts";
        }
        if (definition.getSchema().getFieldIndex("last_update_timestamp").isDefined())
        {
            return "last_update_timestamp";
        }
        if (definition.getSchema().getFieldIndex("ts").isDefined())
        {
            return "ts";
        }
        return definition.getSchema().fields()[0].name();
    }

    private static String normalizeImportMode(String rawMode)
    {
        String mode = rawMode == null ? MODE_DELTA : rawMode.trim().toLowerCase();
        if (!MODE_DELTA.equals(mode) && !MODE_HUDI.equals(mode))
        {
            throw new IllegalArgumentException("pixels.spark.import.mode must be one of: delta, hudi");
        }
        return mode;
    }

    private static String firstNonEmpty(String primary, String fallback)
    {
        if (primary != null && !primary.trim().isEmpty())
        {
            return primary.trim();
        }
        if (fallback != null && !fallback.trim().isEmpty())
        {
            return fallback.trim();
        }
        return null;
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

    private static Dataset<Row> withHudiPartitionColumn(
            Dataset<Row> dataset,
            BenchmarkTableDefinition definition,
            int hudiPartitionCount)
    {
        return PixelsHudiPartitioning.withPartitionColumn(
                dataset,
                definition.getPrimaryKeyColumns(),
                hudiPartitionCount);
    }
}
