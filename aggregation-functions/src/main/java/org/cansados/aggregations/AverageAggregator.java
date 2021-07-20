package org.cansados.aggregations;

import com.mongodb.spark.MongoSpark;
import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class AverageAggregator {
    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));

        SparkSession spark = SparkSession
                .builder()
                .appName("AverageAggregator")
                .config("spark.mongodb.output.uri", argList.remove(0) + "dsid.averages?authSource=admin")
                .getOrCreate();

        Configuration hadoopConfig = spark.sparkContext().hadoopConfiguration();
        hadoopConfig.set("fs.s3a.access.key", argList.remove(0));
        hadoopConfig.set("fs.s3a.secret.key", argList.remove(0));
        hadoopConfig.set("fs.s3a.endpoint", "s3.amazonaws.com");

        String inventoryId = argList.remove(0);

        spark.log().info("Starting spark app...");

        for (String arg : argList) {
            spark.log().info("will download csv from: " + arg);
        }

        try {
            JavaRDD<Row> lines = spark.read().format("csv")
                    .option("sep", ",")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .load(argList.stream().filter(it -> it.startsWith("s3a")).toArray(String[]::new))
                    .toJavaRDD();

            // Group by year
            JavaPairRDD<Integer, Iterable<Row>> byYear = lines.groupBy(row -> {
                String dateString = row.getAs("Date");
                return LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd")).getYear();
            });

            JavaPairRDD<Integer, Double> meanTempByYear = byYear.aggregateByKey(new Tuple2<>(0, 0.0),
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
            ).mapValues(sum -> { return (1.0 * sum._2 / sum._1); });

            // Map results to
            JavaRDD<Document> documents = meanTempByYear.map(tuple -> {
                Map<String, String> valuesMap = Map.of(
                        "inventoryId", inventoryId,
                        "year", tuple._1.toString(),
                        "avg", tuple._2.toString()
                );
                StringSubstitutor substitutor = new StringSubstitutor(valuesMap);
                String templateString = "{ inventoryId: '${inventoryId}', year: ${year}, avg: ${avg} }";
                return Document.parse(substitutor.replace(templateString));
            });

            MongoSpark.save(documents);
        } catch (Exception e) {
            spark.log().error("Spark aggregation function threw an error. Listing args below: ");
            // Remove aws credentials
            argList.stream().filter(it -> !it.startsWith("fs.s3a")).forEachOrdered(arg -> spark.log().error(arg));
            e.printStackTrace();
        }

    }
}
