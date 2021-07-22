package org.cansados.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Util {
    public static SparkSession setupSparkSession(List<String> argList, String appName, String collection) {
        SparkSession session = SparkSession
                .builder()
                .appName(appName)
                .config("spark.mongodb.output.uri", argList.remove(0) + "dsid." + collection + "?authSource=admin")
                .getOrCreate();

        Configuration hadoopConfig = session.sparkContext().hadoopConfiguration();
        hadoopConfig.set("fs.s3a.access.key", argList.remove(0));
        hadoopConfig.set("fs.s3a.secret.key", argList.remove(0));
        hadoopConfig.set("fs.s3a.endpoint", "s3.amazonaws.com");

        return session;
    }

    public static void logFilePaths(SparkSession session, List<String> argList) {
        session.log().info("Starting spark app...");

        for (String arg : argList) {
            session.log().info("will download csv from: " + arg);
        }
    }
}
