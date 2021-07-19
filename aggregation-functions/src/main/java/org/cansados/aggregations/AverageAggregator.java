package org.cansados.aggregations;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

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

        JavaRDD<Row> lines = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(argList.toArray(new String[0]))
                .toJavaRDD();

        // Group by year
        JavaPairRDD<Integer, Iterable<Row>> byYear = lines.groupBy(row -> {
            String dateString = row.getAs("Date");
            return LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd")).getYear();
        });

        List<Tuple2<Integer, Double>> meanTempByYear = byYear.aggregateByKey(new Tuple2<>(0, 0.0),
                (tuple, rows) -> {
                    AtomicReference<Integer> count = new AtomicReference<>(0);
                    AtomicReference<Double> sum = new AtomicReference<>(0.0);
                    rows.iterator().forEachRemaining(row -> {
                        Double current = row.getAs("Mean_Temp");
                        sum.updateAndGet(v -> v + current);
                        count.updateAndGet(v -> v + 1);
                    });
                    return new Tuple2<>(count.get(), sum.get());
                },
                (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)
        ).mapValues(sum -> { return (1.0 * sum._2 / sum._1); }).collect();

        meanTempByYear.forEach(tuple -> spark.log().info("Year: "+ tuple._1 + " | Mean temperature: " + tuple._2));

    }
}
