package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConversions;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class ImportVcfToDataLakeByRanges {

    private static final int PARTITION_SIZE = 100_000;
    private static final int MAX_RECORDS_PER_FILE = 25_000;

    //todo add integration tests!
    public static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];
        String statusPath = args[2];
        String impactPath = args[3];
        String dbSnpPath = args[4];

        SparkSession sparkSession = SparkSession.builder().appName(ImportVcfToDataLakeByRanges.class.getName()).getOrCreate();

        Dataset result = convertVcfsToDatalakeFormatByRanges(sparkSession, inputPath, impactPath, dbSnpPath);

        writeToDataLake(result, outputPath);

        writeStatus(getStatus(sparkSession, inputPath), statusPath);

    }

    static Dataset convertVcfsToDatalakeFormatByRanges(SparkSession spark, String inputPath, String impactPath, String dbSnpPath){

        Dataset table = getMutationsByIndex(spark, inputPath);

        Dataset impact = spark.read().option("sep", "\t").option("header", "true").csv(impactPath);
        impact = impact.withColumn("chrom", concat(lit("chr"), upper(col("chrom")))); //same format as in vcf
        impact = impact.dropDuplicates("chrom", "pos","ref","alt");

        Dataset dbSnp = spark.read().option("sep", "\t").csv(dbSnpPath).where("not _c0 like '#%'");

        dbSnp = dbSnp
                .withColumn("chrom", concat(lit("chr"), upper(col("_c0")))).drop("_c0") //same format as in vcf
                .withColumnRenamed("_c1", "pos")
                .withColumnRenamed("_c2", "ref")
                .withColumnRenamed("_c3", "alt")
                .withColumnRenamed("_c4", "dbSNP");

        Dataset result = table
                .join(impact, JavaConversions.asScalaBuffer(Arrays.asList("chrom", "pos","ref","alt")), "left")
                .join(dbSnp, JavaConversions.asScalaBuffer(Arrays.asList("chrom", "pos","ref","alt")), "left");

        result = result.withColumn("impact", trim(col("IMPACT")));

        result =  result
                .groupBy("chrom", "pos","ref","alt", "impact", "dbSNP")
                .agg(collect_set("hom-struct").as("hom"), (collect_set("het-struct").as("het")));

        result = result
                .withColumn("resp", struct("ref", "alt", "impact", "dbSNP", "hom", "het"))
                .drop("hom", "het");

        result = result
                .withColumn("pos_bucket", floor(col("pos").divide(lit(PARTITION_SIZE))))
                .groupBy("chrom", "pos_bucket", "pos").agg(collect_set(col("resp")).as("entries"));

        return result;
    }

    static Dataset getMutationsByIndex(SparkSession spark, String inputPath){

        Dataset raw = getRawInput(spark, inputPath);

        Dataset table = raw
                .withColumn("homo", when(col("last").startsWith("1/1"), true).otherwise(false))
                .withColumn("srr", split(reverse(split(input_file_name(), "/")).getItem(0), "\\.").getItem(0))
                .withColumn("chrom", split(col("chrom"), "_").getItem(0))
                .withColumn("pos", col("pos").cast(DataTypes.IntegerType))
                .withColumn("qual", col("qual").cast(DataTypes.FloatType))
                .withColumn("ad", split(col("last"), ":").getItem(1))
                ;

        table = table.drop("_c2", "_c5", "_c6", "_c7", "_c8", "_c9", "last");

        table = table
                .withColumn("homo-srr", when(col("homo"), col("srr")))
                .withColumn("hetero-srr", when(not(col("homo")), col("srr")))
                .drop("homo");

        table = table
                .withColumn("hom-struct", when(col("homo-srr").isNotNull(), struct(col("homo-srr").as("id"), col("qual").as("qual"),col("ad").as("ad"))))
                .withColumn("het-struct", when(col("hetero-srr").isNotNull(), struct(col("hetero-srr").as("id"), col("qual").as("qual"),col("ad").as("ad"))))
                .drop("homo-srr", "hetero-srr", "srr", "qual", "ad");

        return table;

    }

    private static Dataset getRawInput(SparkSession spark, String inputPath){

        Dataset raw = spark.read().textFile(inputPath).where("not value like '#%'");

        Dataset table = spark.read().option("sep", "\t").csv(raw);

        return table
                .withColumnRenamed("_c0", "chrom")
                .withColumnRenamed("_c1", "pos")
                .withColumnRenamed("_c3", "ref")
                .withColumnRenamed("_c4", "alt")
                .withColumnRenamed("_c5", "qual")
                .withColumnRenamed("_c9", "last")
                ;

    }

    static void writeToDataLake(Dataset df, String outputPath){

        df
                .repartition(col("chrom"), col("pos_bucket"))
                .write()
                .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
                .mode("overwrite")
                .partitionBy("chrom", "pos_bucket")
                .parquet(outputPath);


    }

    static Dataset getStatus(SparkSession spark, String inputPath){

        Dataset raw = getRawInput(spark, inputPath);

        raw = raw.withColumn("file_name", input_file_name());

        return raw
                .groupBy()
                .agg(
                        countDistinct("chrom", "pos").as("coordinates_num"),
                        countDistinct("chrom", "pos","ref", "alt").as("mutations_num"),
                        countDistinct("file_name").as("samples_num")
                ).withColumn("update_date", current_timestamp().cast(DataTypes.StringType));
    }

    static void writeStatus(Dataset df, String statusPath){
        df.coalesce(1).write().mode(SaveMode.Append).json(statusPath);
    }

}
