package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class ImportVcfToDataLakeByRangesTest {

    static{
        Logger.getLogger("org.apache").setLevel(Level.WARN);
    }

    @Test
    public void convertVcfsToDatalakeFormatWithImpactTest(){

        SparkSession spark = SparkSession.builder().appName("convertVcfsToDatalakeFormatWithImpactTest").master("local[*]").getOrCreate();

        Dataset result19 = ImportVcfToDataLakeByRanges.convertVcfsToDatalakeFormatByRanges(spark, "src/test/resources/input/*/hg19/", "src/test/resources/input/*/Impact/impacts.hg19.csv");

        Assert.assertEquals(1622, result19.count());

        Assert.assertEquals("we should keep only one impact", 1,
                result19.where("chrom = 'chr1' and pos = 11301714").select(functions.size(functions.col("entries"))).as(Encoders.INT()).collectAsList().get(0));

        result19.printSchema();

        result19.show(false);

    }

    @Test
    public void writeToDataLakeTest(){

        SparkSession spark = SparkSession.builder().appName("ImportVcfToDataLakeByRangesTest").master("local[*]").getOrCreate();

        Dataset result19 = ImportVcfToDataLakeByRanges.convertVcfsToDatalakeFormatByRanges(spark, "src/test/resources/input/*/hg19/", "src/test/resources/input/*/Impact/impacts.hg19.csv");

        String outputPath = "target/test-out/" + UUID.randomUUID();

        ImportVcfToDataLakeByRanges.writeToDataLake(result19, outputPath);

        Dataset resultFromDisk = spark.read().parquet(outputPath);

        Assert.assertEquals(result19.count(), resultFromDisk.count());

        resultFromDisk.printSchema();

        resultFromDisk.show();

    }

    @Test
    public void getStatusTest(){
        SparkSession spark = SparkSession.builder().appName("getStatusTest").master("local[*]").getOrCreate();

        Dataset result = ImportVcfToDataLakeByRanges.getStatus(spark, "src/test/resources/input/*/hg19/");

        result.printSchema();
        result.show(false);

        Assert.assertEquals(1, result.count());
        Assert.assertEquals(1, result.where("coordinates_num <= mutations_num and update_date is not null").count());
        Assert.assertEquals(1, result.where("samples_num == 3").count());
    }

    @Test
    public void writeStatusTest(){

        SparkSession spark = SparkSession.builder().appName("writeStatusTest").master("local[*]").getOrCreate();

        Dataset status = ImportVcfToDataLakeByRanges.getStatus(spark, "src/test/resources/input/*/hg19/");

        String outputPath = "target/test-status/" + UUID.randomUUID();

        ImportVcfToDataLakeByRanges.writeStatus(status, outputPath);

        Dataset resultFromDisk = spark.read().json(outputPath);

        Assert.assertEquals(status.count(), resultFromDisk.count());

        resultFromDisk.printSchema();

        resultFromDisk.show();
    }

    @Test
    public void getMutationsByIndexTest(){
        SparkSession spark = SparkSession.builder().appName("getMutationsByIndexTest").master("local[*]").getOrCreate();

        Dataset df = ImportVcfToDataLakeByRanges.getMutationsByIndex(spark, "src/test/resources/input/*/hg19/");

        df.printSchema();

        df.show(false);

    }
}
