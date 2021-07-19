package org.cansados.aggregations;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AverageAggregator {
    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));

        SparkSession spark = SparkSession
                .builder()
                .appName("AverageAggregator")
                .getOrCreate();

        Configuration config = spark.sparkContext().hadoopConfiguration();
        config.set("fs.s3a.access.key", argList.remove(0));
        config.set("fs.s3a.secret.key", argList.remove(0));
        config.set("fs.s3a.endpoint", "s3.amazonaws.com");

        spark.log().info("Starting spark app...");

        for (String arg : argList) {
            spark.log().info("will download csv from: " + arg);
        }

        Dataset<Row> lines = spark.read().csv(argList.toArray(new String[0]));

        spark.log().info("number of lines read:" + lines.count());
    }
}
