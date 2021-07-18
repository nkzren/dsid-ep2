package org.cansados.aggregations;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AverageAggregator {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();
        spark.log().info("Starting spark app...");

        for (String arg : args) {
            spark.log().info("will download csv from: " + arg);
        }

        JavaRDD<Row> lines = spark.read().csv(args).javaRDD();

        System.out.println("number of lines read:" + lines.count());
    }
}
