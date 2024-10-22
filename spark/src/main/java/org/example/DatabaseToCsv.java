package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

public class DatabaseToCsv {
    public static void main(String[] args) {
        System.setProperty("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED");

        SparkSession spark = SparkSession.builder()
                .appName("Database to CSV")
                .master("local[*]")
                .getOrCreate();

//        SparkSession spark = SparkSession.builder()
//                .appName("Database to CSV")
//                .master("local[*]")
//                .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
//                .getOrCreate();

        String jdbcUrl = "jdbc:mysql://localhost:3306/test_db";
        String tableName = "employeestst";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root");
        connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver");

        // Read data from the database
        Dataset<Row> dataFrame = spark.read()
                .jdbc(jdbcUrl, tableName, connectionProperties);

        dataFrame.write()
                .mode(SaveMode.Overwrite)
                .csv("C:\\data\\output");

        // Stop the Spark session
        spark.stop();
    }
}