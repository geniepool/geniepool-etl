package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConversions;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class ImportVcfToDataLakeByRanges {


    //todo add integration tests!
    public static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];
        String statusPath = args[2];
        String impactPath = args[3];

        SparkSession sparkSession = SparkSession.builder().appName(ImportVcfToDataLakeByRanges.class.getName()).getOrCreate();

        Dataset result = convertVcfsToDatalakeFormatByRanges(sparkSession, inputPath, impactPath);

        writeToDataLake(result, outputPath);

        writeStatus(getStatus(sparkSession, inputPath), statusPath);

    }

    static Dataset convertVcfsToDatalakeFormatByRanges(SparkSession spark, String inputPath, String impactPath){

        Dataset table = getMutationsByIndex(spark, inputPath);

        Dataset impact = spark.read().option("sep", "\t").option("header", "true").csv(impactPath);
        impact = impact.dropDuplicates("chrom", "pos","ref","alt");

        Dataset tableWithImpact = table.join(impact, JavaConversions.asScalaBuffer(Arrays.asList("chrom", "pos","ref","alt")), "left");
        tableWithImpact = tableWithImpact.withColumn("impact", trim(col("IMPACT")));

        tableWithImpact.printSchema();
        tableWithImpact.show();

        tableWithImpact =  tableWithImpact
                .groupBy("chrom", "pos","ref","alt", "impact")
                .agg(collect_set("hom-struct").as("hom"), (collect_set("het-struct").as("het")));

        tableWithImpact = tableWithImpact
                .withColumn("resp", struct("ref", "alt", "impact", "hom", "het"))
                .drop("hom", "het");

        tableWithImpact = tableWithImpact
                .withColumn("pos_bucket", floor(col("pos").divide(lit(1_000_000))))
                .groupBy("chrom", "pos_bucket", "pos").agg(collect_set(col("resp")).as("entries"));

        return tableWithImpact;
    }

    static Dataset getMutationsByIndex(SparkSession spark, String inputPath){

        Dataset raw = getRawInput(spark, inputPath);

        Dataset table = raw
                .withColumn("homo", when(col("_c9").startsWith("1/1"), true).otherwise(false))
                .withColumn("srr", split(reverse(split(input_file_name(), "/")).getItem(0), "\\.").getItem(0))
                .withColumn("chrom", split(col("chrom"), "_").getItem(0))
                .withColumn("pos", col("pos").cast(DataTypes.IntegerType))
                .withColumn("qual", col("qual").cast(DataTypes.FloatType));

        table = table.drop("_c2", "_c5", "_c6", "_c7", "_c8", "_c9");

        table = table
                .withColumn("homo-srr", when(col("homo"), col("srr")))
                .withColumn("hetero-srr", when(not(col("homo")), col("srr")))
                .drop("homo");

        table = table
                .withColumn("hom-struct", when(col("homo-srr").isNotNull(), struct(col("homo-srr").as("id"), col("qual").as("qual"))))
                .withColumn("het-struct", when(col("hetero-srr").isNotNull(), struct(col("hetero-srr").as("id"), col("qual").as("qual"))))
                .drop("homo-srr", "hetero-srr", "srr", "qual");

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
                ;

    }

    static void writeToDataLake(Dataset df, String outputPath){

        df
                .repartition(col("chrom"), col("pos_bucket"))
                .write().mode("overwrite")
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
