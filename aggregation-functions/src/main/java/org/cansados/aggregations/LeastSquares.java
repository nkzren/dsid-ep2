package org.cansados.aggregations;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.cansados.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LeastSquares {
    public static void main(String[] args) {

        List<String> argList = new ArrayList<>(Arrays.asList(args));

        SparkSession session = Util.setupSparkSession(
                argList,
                "LeastSquares",
                "predicted_averages"
        );

        String inventoryId = argList.remove(0);

        String columnToPredict = argList.remove(0);

        String auxColumn = argList.remove(0);

        Util.logFilePaths(session, argList);

        try {
            Dataset<Row> lines = session.read().format("csv")
                    .option("sep", ",")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .load(argList.stream().filter(it -> it.startsWith("s3a")).toArray(String[]::new));


            Dataset<Row> averagesByYear = lines
                    .groupBy(
                            new Column("Year").as("year"),
                            new Column("ID").as("inventoryId"),
                            new Column(auxColumn)
                    )
                    .agg(functions.mean(columnToPredict).as("avg"))
                    .orderBy("Year");

            JavaRDD<Row> predictedValues = new VectorAssembler()
                    .setInputCols(new String[]{auxColumn})
                    .setOutputCol("predicted_avg")
                    .transform(averagesByYear).toJavaRDD();

            MongoSpark.save(predictedValues.map(row -> {
                session.log().info(row.getAs("predicted_avg"));
            }));
        } catch (Exception e) {
            session.log().error("Spark aggregation function threw an error. Listing args below: ");
            // Remove aws credentials
            argList.stream().filter(it -> !it.startsWith("fs.s3a")).forEachOrdered(arg -> session.log().error(arg));
            e.printStackTrace();
        }
    }
}
