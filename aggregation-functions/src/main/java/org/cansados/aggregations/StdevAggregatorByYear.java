package org.cansados.aggregations;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cansados.util.Util;

import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StdevAggregatorByYear {
    public static void main(String[] args) {
        List<String> argList = new ArrayList<>(Arrays.asList(args));

        SparkSession session = Util.setupSparkSession(
                argList,
                "StdevAggregatorByYear",
                "stdev"
        );

        String inventoryId = argList.remove(0);

        String columnName = argList.remove(0);

        Util.logFilePaths(session, argList);

        try {
            Dataset<Row> lines = session.read().format("csv")
                    .option("sep", ",")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .load(argList.stream().filter(it -> it.startsWith("s3a")).toArray(String[]::new));


            MongoSpark.save(lines
                    .groupBy(
                            new Column("Year").as("year"),
                            new Column("ID").as("inventoryId")
                    )
                    .agg(functions.stddev(columnName).as("stdev"))
                    .orderBy("Year")
            );
        } catch (Exception e) {
            session.log().error("Spark aggregation function threw an error. Listing args below: ");
            // Remove aws credentials
            argList.stream().filter(it -> !it.startsWith("fs.s3a")).forEachOrdered(arg -> session.log().error(arg));
            e.printStackTrace();
        }
    }
}
