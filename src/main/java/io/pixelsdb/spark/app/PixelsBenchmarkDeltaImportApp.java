package io.pixelsdb.spark.app;

import io.pixelsdb.spark.bucket.PixelsBucketId;
import io.pixelsdb.spark.config.PixelsSparkConfig;
import io.pixelsdb.spark.merge.PixelsDeltaMergeColumns;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
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

    private static final TableSpec[] TABLE_SPECS = new TableSpec[] {
            new TableSpec("customer", "customer.csv", new StructType(new StructField[] {
                    field("custID", DataTypes.IntegerType),
                    field("companyID", DataTypes.IntegerType),
                    field("gender", DataTypes.StringType),
                    field("name", DataTypes.StringType),
                    field("age", DataTypes.IntegerType),
                    field("phone", DataTypes.StringType),
                    field("province", DataTypes.StringType),
                    field("city", DataTypes.StringType),
                    field("loan_balance", DataTypes.FloatType),
                    field("saving_credit", DataTypes.IntegerType),
                    field("checking_credit", DataTypes.IntegerType),
                    field("loan_credit", DataTypes.IntegerType),
                    field("Isblocked", DataTypes.IntegerType),
                    field("created_date", DataTypes.DateType),
                    field("last_update_timestamp", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            }), "custID"),
            new TableSpec("company", "company.csv", new StructType(new StructField[] {
                    field("companyID", DataTypes.IntegerType),
                    field("name", DataTypes.StringType),
                    field("category", DataTypes.StringType),
                    field("staff_size", DataTypes.IntegerType),
                    field("loan_balance", DataTypes.FloatType),
                    field("phone", DataTypes.StringType),
                    field("province", DataTypes.StringType),
                    field("city", DataTypes.StringType),
                    field("saving_credit", DataTypes.IntegerType),
                    field("checking_credit", DataTypes.IntegerType),
                    field("loan_credit", DataTypes.IntegerType),
                    field("Isblocked", DataTypes.IntegerType),
                    field("created_date", DataTypes.DateType),
                    field("last_update_timestamp", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            }), "companyID"),
            new TableSpec("savingaccount", "savingAccount.csv", new StructType(new StructField[] {
                    field("accountID", DataTypes.IntegerType),
                    field("userID", DataTypes.IntegerType),
                    field("balance", DataTypes.FloatType),
                    field("Isblocked", DataTypes.IntegerType),
                    field("ts", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            }), "accountID"),
            new TableSpec("checkingaccount", "checkingAccount.csv", new StructType(new StructField[] {
                    field("accountID", DataTypes.IntegerType),
                    field("userID", DataTypes.IntegerType),
                    field("balance", DataTypes.FloatType),
                    field("Isblocked", DataTypes.IntegerType),
                    field("ts", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            }), "accountID"),
            new TableSpec("transfer", "transfer.csv", new StructType(new StructField[] {
                    field("id", DataTypes.LongType),
                    field("sourceID", DataTypes.IntegerType),
                    field("targetID", DataTypes.IntegerType),
                    field("amount", DataTypes.FloatType),
                    field("type", DataTypes.StringType),
                    field("ts", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            }), "id"),
            new TableSpec("checking", "checking.csv", new StructType(new StructField[] {
                    field("id", DataTypes.IntegerType),
                    field("sourceID", DataTypes.IntegerType),
                    field("targetID", DataTypes.IntegerType),
                    field("amount", DataTypes.FloatType),
                    field("type", DataTypes.StringType),
                    field("ts", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            }), "id"),
            new TableSpec("loanapps", "loanApps.csv", new StructType(new StructField[] {
                    field("id", DataTypes.IntegerType),
                    field("applicantID", DataTypes.IntegerType),
                    field("amount", DataTypes.FloatType),
                    field("duration", DataTypes.IntegerType),
                    field("status", DataTypes.StringType),
                    field("ts", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            }), "id"),
            new TableSpec("loantrans", "loanTrans.csv", new StructType(new StructField[] {
                    field("id", DataTypes.IntegerType),
                    field("applicantID", DataTypes.IntegerType),
                    field("appID", DataTypes.IntegerType),
                    field("amount", DataTypes.FloatType),
                    field("status", DataTypes.StringType),
                    field("ts", DataTypes.TimestampType),
                    field("duration", DataTypes.IntegerType),
                    field("contract_timestamp", DataTypes.TimestampType),
                    field("delinquency", DataTypes.IntegerType),
                    field("freshness_ts", DataTypes.TimestampType)
            }), "id")
    };

    public static void main(String[] args)
    {
        String csvRoot = args.length > 0 ? args[0] : "/home/antio2/projects/pixels-benchmark/Data_1x";
        String deltaRoot = args.length > 1 ? args[1]
                : PixelsSparkConfig.instance().getOrDefault("pixels.import.local.delta-root",
                "/home/ubuntu/disk1/tmp/pixels-benchmark-deltalake/data_1x");
        String sparkMaster = args.length > 2 ? args[2] : PixelsSparkConfig.instance().get(PixelsSparkConfig.SPARK_MASTER);
        Set<String> selectedTables = parseSelectedTables(args.length > 3 ? args[3] : null);
        int chunkRows = PixelsSparkConfig.instance()
                .getIntOrDefault(PixelsSparkConfig.IMPORT_CSV_CHUNK_ROWS, 2560000);
        boolean countRows = PixelsSparkConfig.instance()
                .getBooleanOrDefault(PixelsSparkConfig.IMPORT_COUNT_ROWS, false);
        int bucketCount = PixelsSparkConfig.instance().getIntOrDefault("node.bucket.num", 0);
        boolean enableDeletionVectors = PixelsSparkConfig.instance()
                .getBooleanOrDefault(PixelsSparkConfig.DELTA_ENABLE_DELETION_VECTORS, true);

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
            for (TableSpec spec : TABLE_SPECS)
            {
                if (selectedTables != null && !selectedTables.contains(spec.tableName))
                {
                    continue;
                }
                String csvPath = csvRoot + "/" + spec.fileName;
                String deltaPath = deltaRoot + "/" + spec.tableName;
                long rowCount = importTableInChunks(
                        spark,
                        spec,
                        csvPath,
                        deltaPath,
                        bucketCount,
                        enableDeletionVectors,
                        chunkRows,
                        countRows);

                System.out.println("table=" + spec.tableName
                        + " csv_path=" + csvPath
                        + " delta_path=" + deltaPath
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
            TableSpec spec,
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
                    .schema(spec.schema)
                    .option("header", "false")
                    .option("mode", "FAILFAST")
                    .option("nullValue", "")
                    .load(csvPath);
            Dataset<Row> output = withHashBucketColumn(dataset, spec, bucketCount);
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
                        .schema(spec.schema)
                        .option("header", "false")
                        .option("mode", "FAILFAST")
                        .option("nullValue", "")
                        .load(chunkPath.toString());

                Dataset<Row> output = withHashBucketColumn(chunk, spec, bucketCount);
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

    private static Dataset<Row> withHashBucketColumn(Dataset<Row> dataset, TableSpec spec, int bucketCount)
    {
        return PixelsBucketId.withBucketColumnFromSchema(dataset, spec.primaryKeyColumns, spec.schema, bucketCount);
    }

    private static StructField field(String name, org.apache.spark.sql.types.DataType dataType)
    {
        return DataTypes.createStructField(name, dataType, true, Metadata.empty());
    }

    private static final class TableSpec
    {
        private final String tableName;
        private final String fileName;
        private final StructType schema;
        private final List<String> primaryKeyColumns;

        private TableSpec(String tableName, String fileName, StructType schema, String... primaryKeyColumns)
        {
            this.tableName = tableName;
            this.fileName = fileName;
            this.schema = schema;
            this.primaryKeyColumns = Arrays.asList(primaryKeyColumns);
        }
    }
}
