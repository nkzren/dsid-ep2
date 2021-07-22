package org.cansados.aggregations;

import com.mongodb.spark.MongoSpark;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.cansados.util.Util;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class StdevAggregatorByMonth {
    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));

        SparkSession session = Util.setupSparkSession(
                argList,
                "AverageAggregatorByMonth",
                "averages"
        );

        String inventoryId = argList.remove(0);

        String columnName = argList.remove(0);

        Util.logFilePaths(session, argList);

        try {
            JavaRDD<Row> lines = session.read().format("csv")
                    .option("sep", ",")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .load(argList.stream().filter(it -> it.startsWith("s3a")).toArray(String[]::new))
                    .toJavaRDD();

            // Group by month
            JavaPairRDD<String, Iterable<Row>> byMonth = lines.groupBy(row -> {
                String month = row.getAs("Month");
                String year = row.getAs("Year");
                return year + "/" + month;
            });

            JavaPairRDD<String, Double> meanTempByMonth = byMonth.aggregateByKey(new Tuple2<>(0, 0.0),
                    (tuple, rows) -> {
                        AtomicReference<Integer> count = new AtomicReference<>(0);
                        AtomicReference<Double> sum = new AtomicReference<>(0.0);
                        rows.iterator().forEachRemaining(row -> {
                            Double current = row.getAs(columnName);
                            sum.updateAndGet(v -> v + current);
                            count.updateAndGet(v -> v + 1);
                        });
                        return new Tuple2<>(count.get(), sum.get());
                    },
                    (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)
            ).mapValues(sum -> (1.0 * sum._2 / sum._1));

            // Map results to
            JavaRDD<Document> documents = meanTempByMonth.map(tuple -> {
                Map<String, String> valuesMap = Map.of(
                        "inventoryId", inventoryId,
                        "groupedBy", "month",
                        "label", tuple._1,
                        "avg", tuple._2.toString()
                );
                StringSubstitutor substitutor = new StringSubstitutor(valuesMap);
                String templateString = "{ inventoryId: '${inventoryId}', groupedBy: 'month', label: '${label}', avg: ${avg} }";
                return Document.parse(substitutor.replace(templateString));
            });

            MongoSpark.save(documents);
        } catch (Exception e) {
            session.log().error("Spark aggregation function threw an error. Listing args below: ");
            // Remove aws credentials
            argList.stream().filter(it -> !it.startsWith("fs.s3a")).forEachOrdered(arg -> session.log().error(arg));
            e.printStackTrace();
        }
    }
}
