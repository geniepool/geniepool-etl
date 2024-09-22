package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.createStructField;

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
        boolean t2t = Boolean.parseBoolean(args[5]);
        String gnomadPath = args[6];
        String alphaPath = args[7];

        SparkSession sparkSession = SparkSession.builder().appName(ImportVcfToDataLakeByRanges.class.getName()).getOrCreate();

        Dataset result = convertVcfsToDatalakeFormatByRanges(sparkSession, inputPath, impactPath, dbSnpPath, t2t, gnomadPath, alphaPath);

        writeToDataLake(result, outputPath);

        writeStatus(getStatus(sparkSession, inputPath), statusPath);

    }

    static Dataset convertVcfsToDatalakeFormatByRanges(SparkSession spark, String inputPath, String impactPath,
                                                       String dbSnpPath, boolean t2t, String gnomAdPath, String alphaPath){

        Dataset table = getMutationsByIndex(spark, inputPath);

        Dataset impact = spark.read().option("sep", "\t").option("header", "true").csv(impactPath);
        impact = impact.withColumn("chrom", concat(lit("chr"), upper(col("chrom")))); //same format as in vcf
        impact = impact.dropDuplicates("chrom", "pos","ref","alt");

        Dataset dbSnp = getDBSNP(spark, dbSnpPath, t2t);

        Dataset result = table
                .join(impact, JavaConversions.asScalaBuffer(Arrays.asList("chrom", "pos","ref","alt")), "left")
                .join(dbSnp, JavaConversions.asScalaBuffer(Arrays.asList("chrom", "pos","ref","alt")), "left");

        result = addGnomAd(gnomAdPath, result);

        result = result.withColumn("impact", trim(col("IMPACT")));

        result = addAlpha(result, alphaPath);

        result =  result
                .groupBy("chrom", "pos","ref","alt", "impact", "dbSNP", "gnomad_an", "gnomad_ac", "gnomad_nhomalt", "hg38_coordinate", "alphamissense")
                .agg(collect_set("hom-struct").as("hom"), (collect_set("het-struct").as("het")));

        result = result
                .withColumn("resp", struct("ref", "alt", "impact", "dbSNP",
                        "gnomad_an", "gnomad_ac", "gnomad_nhomalt",
                        "hg38_coordinate", "alphamissense", "hom", "het"))
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

    private static Dataset getDBSNP(SparkSession spark, String inputPath, boolean t2t){
        Dataset result;

        if (t2t){
            StructType schema = DataTypes.createStructType(new StructField[]{
                    createStructField("POS", DataTypes.LongType, true),
                    createStructField("REF", DataTypes.StringType, true),
                    createStructField("ALT", DataTypes.StringType, true),
                    createStructField("SNP", DataTypes.StringType, true),
            });

            result = spark.read().schema(schema).parquet(inputPath);

            result = result.withColumn("chrom", concat(lit("chr"),
                    upper(
                            regexp_replace(substring_index(
                                    substring_index(reverse(substring_index(reverse(input_file_name()), "/", 1)), ".", 1),
                                    "_", 1
                            ), "c", "")
                    )
                    )
            );

            result = result
                    .withColumnRenamed("POS", "pos")
                    .withColumnRenamed("REF", "ref")
                    .withColumnRenamed("ALT", "alt")
                    .withColumnRenamed("SNP", "dbSNP");
        }else{
            result = spark.read().option("sep", "\t").csv(inputPath).where("not _c0 like '#%'");

            result = result
                    .withColumn("chrom", concat(lit("chr"), upper(col("_c0")))).drop("_c0") //same format as in vcf
                    .withColumnRenamed("_c1", "pos")
                    .withColumnRenamed("_c2", "ref")
                    .withColumnRenamed("_c3", "alt")
                    .withColumnRenamed("_c4", "dbSNP");
        }

        return result;
    }

    static Dataset addAlpha(Dataset df, String alphaPath){

        Dataset alpha = df.sparkSession().read().parquet(alphaPath);

        alpha = alpha
                .withColumn("chrom", concat(lit("chr"),
                        upper(substring_index(reverse(substring_index(reverse(input_file_name()), "/", 1)), ".", 1))))
                .withColumnRenamed("POS", "pos");

        Dataset result = df.join(alpha, JavaConversions.asScalaBuffer(Arrays.asList("chrom", "pos")), "left");

        result = result.withColumn("alphamissense",
                when(col("ref").equalTo("C").and(col("C").equalTo(0).and(col("alt").equalTo("T"))), col("T")).
                        when(col("ref").equalTo("C").and(col("C").equalTo(0).and(col("alt").equalTo("A"))), col("A")).
                        when(col("ref").equalTo("C").and(col("C").equalTo(0).and(col("alt").equalTo("G"))), col("G")).

                        when(col("ref").equalTo("A").and(col("A").equalTo(0).and(col("alt").equalTo("C"))), col("C")).
                        when(col("ref").equalTo("A").and(col("A").equalTo(0).and(col("alt").equalTo("T"))), col("T")).
                        when(col("ref").equalTo("A").and(col("A").equalTo(0).and(col("alt").equalTo("G"))), col("G")).

                        when(col("ref").equalTo("T").and(col("T").equalTo(0).and(col("alt").equalTo("A"))), col("A")).
                        when(col("ref").equalTo("T").and(col("T").equalTo(0).and(col("alt").equalTo("C"))), col("C")).
                        when(col("ref").equalTo("T").and(col("T").equalTo(0).and(col("alt").equalTo("G"))), col("G")).

                        when(col("ref").equalTo("G").and(col("G").equalTo(0).and(col("alt").equalTo("A"))), col("A")).
                        when(col("ref").equalTo("G").and(col("G").equalTo(0).and(col("alt").equalTo("C"))), col("C")).
                        when(col("ref").equalTo("G").and(col("G").equalTo(0).and(col("alt").equalTo("T"))), col("T"))
        );

        result = result.select("alphamissense", df.columns());

        return result;

    }

    private static Dataset addGnomAd(String gnomAdPath, Dataset df){

        StructType schema = DataTypes.createStructType(new StructField[]{
                createStructField("POS", DataTypes.LongType, true),
                createStructField("REF", DataTypes.StringType, true),
                createStructField("ALT", DataTypes.StringType, true),
                createStructField("gnomad_an", DataTypes.LongType, true),
                createStructField("gnomad_ac", DataTypes.LongType, true),
                createStructField("gnomad_nhomalt", DataTypes.LongType, true),
                createStructField("hg38_coordinates", DataTypes.StringType, true)
        });

        Dataset gnomAd4 = df.sparkSession().read().schema(schema).parquet(gnomAdPath);

        gnomAd4 = gnomAd4.withColumn("chrom", concat(lit("chr"),
                        upper(
                                regexp_replace(substring_index(
                                        substring_index(reverse(substring_index(reverse(input_file_name()), "/", 1)), ".", 1),
                                        "_", 1
                                ), "c", "")
                        )
                )
        );

        gnomAd4 = gnomAd4
                .withColumnRenamed("POS", "pos")
                .withColumnRenamed("REF", "ref")
                .withColumnRenamed("ALT", "alt")
                .withColumnRenamed("hg38_coordinates", "hg38_coordinate");

        return df.join(gnomAd4, JavaConversions.asScalaBuffer(Arrays.asList("chrom", "pos","ref","alt")), "left");
    }

}
