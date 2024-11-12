package org.example;

import java.sql.*;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) {

        //String csvFilePath = "C:\\Users\\nehashukla1306\\Downloads\\sample-csv-files-sample4.csv";  // Replace with your actual file path


        String url = "jdbc:duckdb:C:\\Users\\nehashukla1306\\Downloads\\sample.duckdb"; // In-memory database


        //String createTableSQL = "CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR)";
        //String loadCSVSQL = "COPY users FROM '" + csvFilePath + "' (DELIMITER ',', HEADER TRUE)";

        int numThreads = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);


        for (int i = 0; i < numThreads; i++) {
            executorService.submit(new DatabaseReader(url, i));
        }


        executorService.shutdown();

        try {

            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Tasks did not finish in the expected time.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    static class DatabaseReader implements Runnable {
        private String url;
        private int instanceId;

        public DatabaseReader(String url, int instanceId) {
            this.url = url;
            this.instanceId = instanceId;
        }

        @Override
        public void run() {

            try (Connection conn = DriverManager.getConnection(url)) {

                String query = "SELECT * FROM users";

                try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
                    System.out.println("Thread-" + instanceId + " - Querying database...");

                    // Process the result set
                    while (rs.next()) {
                        int id = rs.getInt("id");
                        String name = rs.getString("name");
                        System.out.println("Thread-" + instanceId + " - ID: " + id + ", Name: " + name);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}