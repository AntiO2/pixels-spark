package io.pixelsdb.spark.app;

import io.pixelsdb.spark.config.PixelsSparkConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class PixelsBenchmarkDeltaImportApp
{
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
            })),
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
            })),
            new TableSpec("savingAccount", "savingAccount.csv", new StructType(new StructField[] {
                    field("accountID", DataTypes.IntegerType),
                    field("userID", DataTypes.IntegerType),
                    field("balance", DataTypes.FloatType),
                    field("Isblocked", DataTypes.IntegerType),
                    field("ts", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            })),
            new TableSpec("checkingAccount", "checkingAccount.csv", new StructType(new StructField[] {
                    field("accountID", DataTypes.IntegerType),
                    field("userID", DataTypes.IntegerType),
                    field("balance", DataTypes.FloatType),
                    field("Isblocked", DataTypes.IntegerType),
                    field("ts", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            })),
            new TableSpec("transfer", "transfer.csv", new StructType(new StructField[] {
                    field("id", DataTypes.LongType),
                    field("sourceID", DataTypes.IntegerType),
                    field("targetID", DataTypes.IntegerType),
                    field("amount", DataTypes.FloatType),
                    field("type", DataTypes.StringType),
                    field("ts", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            })),
            new TableSpec("checking", "checking.csv", new StructType(new StructField[] {
                    field("id", DataTypes.IntegerType),
                    field("sourceID", DataTypes.IntegerType),
                    field("targetID", DataTypes.IntegerType),
                    field("amount", DataTypes.FloatType),
                    field("type", DataTypes.StringType),
                    field("ts", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            })),
            new TableSpec("loanapps", "loanApps.csv", new StructType(new StructField[] {
                    field("id", DataTypes.IntegerType),
                    field("applicantID", DataTypes.IntegerType),
                    field("amount", DataTypes.FloatType),
                    field("duration", DataTypes.IntegerType),
                    field("status", DataTypes.StringType),
                    field("ts", DataTypes.TimestampType),
                    field("freshness_ts", DataTypes.TimestampType)
            })),
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
            }))
    };

    public static void main(String[] args)
    {
        String csvRoot = args.length > 0 ? args[0] : "/home/antio2/projects/pixels-benchmark/Data_1x";
        String deltaRoot = args.length > 1 ? args[1] : "/tmp/pixels-benchmark-deltalake/data_1x";
        String sparkMaster = args.length > 2 ? args[2] : PixelsSparkConfig.instance().get(PixelsSparkConfig.SPARK_MASTER);

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
                String csvPath = csvRoot + "/" + spec.fileName;
                String deltaPath = deltaRoot + "/" + spec.tableName;
                Dataset<Row> dataset = spark.read()
                        .format("csv")
                        .schema(spec.schema)
                        .option("header", "false")
                        .option("mode", "FAILFAST")
                        .option("nullValue", "")
                        .load(csvPath);

                long rowCount = dataset.count();
                dataset.write()
                        .format("delta")
                        .mode("overwrite")
                        .save(deltaPath);

                System.out.println("table=" + spec.tableName
                        + " csv_path=" + csvPath
                        + " delta_path=" + deltaPath
                        + " row_count=" + rowCount);
            }
        }
        finally
        {
            spark.stop();
        }
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

        private TableSpec(String tableName, String fileName, StructType schema)
        {
            this.tableName = tableName;
            this.fileName = fileName;
            this.schema = schema;
        }
    }
}
