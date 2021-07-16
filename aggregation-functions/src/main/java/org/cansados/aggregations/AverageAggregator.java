package org.cansados.aggregations;

import org.apache.spark.sql.SparkSession;

public class AverageAggregator {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();
    }
}
