package com.yugabyte.ysql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.yugabyte.ysql.LoadBalanceProperties.CONNECTION_MANAGER_MAP;

public class TestApp {

    private static int numConnectionsPerThread = 6;
    private static int numThreads = 25;
    public static final String path = System.getenv("YBDB_PATH");

    public static void main(String[] args) throws SQLException, InterruptedException {
        simpleApp();
//        performanceApp(false);
//        performanceApp(true);
    }

    private static void simpleApp() throws SQLException {
        String url = "jdbc:yugabytedb://localhost/yugabyte?load_balance=true";

        Connection connection = DriverManager.getConnection(url, "yugabyte", "yugabyte");
        String ddl = "CREATE TABLE employee (id int, name varchar)";
        Statement statement = connection.createStatement();
        statement.execute(ddl);

        String insert = "INSERT INTO employee VALUES (1, 'C. Bose')";
        statement.executeUpdate(insert);

        String select = "SELECT name FROM employee WHERE id = 1";
        ResultSet resultSet = statement.executeQuery(select);
        while (resultSet.next()) {
            System.out.println("name: " + resultSet.getString(1));
        }
    }

    private static void performanceApp(boolean serial) throws SQLException, InterruptedException {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalStateException("YBDB_PATH not defined.");
        }
        int total = numThreads * numConnectionsPerThread;
        Map<String, Integer> expected1 = expectedInput(total/3 + 1, total/3, total/3);
        // &loggerLevel=DEBUG";
        String baseUrl = "jdbc:yugabytedb://localhost:5433/yugabyte?load-balance=true"
                + "&" + LoadBalanceProperties.REFRESH_INTERVAL_KEY + "=300";
        if (serial) {
            testSingleThreadConnectionCreations(baseUrl, expected1, "127.0.0.1");
        } else {
            testConcurrentConnectionCreations(baseUrl, expected1, "127.0.0.1");
        }
    }

    static Map<String, Integer> expectedInput(int... counts) {
        Map<String, Integer> input = new HashMap<>();
        int s = 1;
        for (int i : counts) {
            input.put("127.0.0." + s, i);
            s++;
        }
        return input;
    }

    public static void startYBDBCluster() {
        executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 15);
        executeCmd(path + "/bin/yb-ctl start --rf 3 --placement_info \"aws.us-west.us-west-2a,aws" +
                ".us-west.us-west-2b,aws.us-west.us-west-2c\"", "Start YugabyteDB rf=3 cluster", 60);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
        }
    }

    private static void testConcurrentConnectionCreations(String url, Map<String,
            Integer> expected1, String controlHost) throws SQLException, InterruptedException {
        System.out.println("Running testConcurrentConnectionCreations() with url " + url);
        startYBDBCluster();
        System.out.println("Cluster started!");
        Thread.sleep(5000);
        try {
            for (int iteration = 0; iteration < 10; iteration++) {
                Thread[] threads = new Thread[numThreads];
                Connection[][] connections = new Connection[numThreads][numConnectionsPerThread];

                for (int i = 0; i < numThreads; i++) {
                    final int j = i;
                    threads[i] = new Thread(() -> {
                        try {
                            for (int k = 0; k < numConnectionsPerThread; k++) {
                                connections[j][k] = DriverManager.getConnection(url, "yugabyte", "yugabyte");
                            }
                        } catch (SQLException e) {
                            System.out.println("getConnection() failed: " + e);
                        }
                    });
                }

//                System.out.println("Launching " + numThreads + " threads to create " + numConnectionsPerThread + " connections each");
                long start = System.currentTimeMillis();
                for (int i = 0; i < numThreads; i++) {
                    threads[i].setDaemon(true);
                    threads[i].start();
                }

                for (int i = 0; i < numThreads; i++) {
                    threads[i].join();
                }
                long elapsed = System.currentTimeMillis() - start;
                System.out.println("[Iteration " + iteration + "] All " + numThreads + " threads completed their tasks in " + elapsed + " ms");

                for (Map.Entry<String, Integer> e : expected1.entrySet()) {
                    verifyOn(e.getKey(), e.getValue(), controlHost);
                    System.out.print(", ");
                }

//                System.out.println("Closing connections ...");
                for (int i = 0; i < numThreads; i++) {
                    for (int j = 0; j < numConnectionsPerThread; j++) {
                        connections[i][j].close();
                    }
                }
            }

        } finally {
            CONNECTION_MANAGER_MAP.clear();
            executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 15);
        }
    }

    private static void testSingleThreadConnectionCreations(String url, Map<String,
            Integer> expected1, String controlHost) throws SQLException, InterruptedException {
        System.out.println("\nRunning testSingleThreadConnectionCreations() with url " + url);
        startYBDBCluster();
        System.out.println("Cluster started!");
        Thread.sleep(5000);
        try {
            for (int iteration = 0; iteration < 1; iteration++) {
                int total = 150;
                Connection[] connections = new Connection[total];

    //            System.out.println("Creating " + total + " connections ...");
                long start = System.currentTimeMillis();
                for (int i = 0 ; i < total; i++) {
                    try {
                        connections[i] = DriverManager.getConnection(url, "yugabyte", "yugabyte");
                    } catch (SQLException e) {
                        System.out.println("[" + i + "] getConnection() failed: " + e);
                    }
                }
                long elapsed = System.currentTimeMillis() - start;
                System.out.println("[Iteration " + iteration + "] All " + total + " connections created in " + elapsed + " ms");

                for (Map.Entry<String, Integer> e : expected1.entrySet()) {
                    verifyOn(e.getKey(), e.getValue(), controlHost);
                    System.out.print(", ");
                }

                System.out.println("Closing connections ...");
                for (int i = 0 ; i < total; i++) {
                    connections[i].close();
                }
            }
        } finally {
            CONNECTION_MANAGER_MAP.clear();
            executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 15);
        }
    }

    public static void executeCmd(String cmd, String msg, int timeout) {
        try {
            ProcessBuilder builder = new ProcessBuilder();
            builder.command("sh", "-c", cmd);
            Process process = builder.start();
            process.waitFor(timeout, TimeUnit.SECONDS);
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                String result = new BufferedReader(new InputStreamReader(process.getInputStream()))
                        .lines().collect(Collectors.joining("\n"));
                throw new RuntimeException(msg + ": FAILED" + "\n" + result);
            }
            System.out.println(msg + ": SUCCEEDED!");
        } catch (Exception e) {
            System.out.println("Exception " + e);
        }
    }

    public static void verifyOn(String server, int expectedCount, String multipurposeParam) {
        try {
            ProcessBuilder builder = new ProcessBuilder();
            builder.command("sh", "-c", "curl http://" + server + ":13000/rpcz");
            Process process = builder.start();
            String result = new BufferedReader(new InputStreamReader(process.getInputStream()))
                    .lines().collect(Collectors.joining("\n"));
            process.waitFor(10, TimeUnit.SECONDS);
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                String out = new BufferedReader(new InputStreamReader(process.getInputStream()))
                        .lines().collect(Collectors.joining("\n"));
                throw new RuntimeException("Could not access /rpcz on " + server + "\n" + out);
            }
            String[] count = result.split("client backend");
            System.out.print(server + " = " + (count.length - 1));
            // Server side validation
            if (expectedCount != (count.length - 1)) {
                throw new RuntimeException("Client backend processes did not match for " + server + ", " +
                        "(expected, actual): " + expectedCount + ", " + (count.length - 1));
            }
            // Client side validation
            if ("skip".equals(multipurposeParam)) {
                return;
            }
            int recorded = LoadBalanceService.getLoad(server);
            if (server.equalsIgnoreCase(multipurposeParam)) {
                // Account for control connection
                expectedCount -= 1;
            }
            if (recorded != expectedCount) {
                throw new RuntimeException("Client side connection count did not match for " + server +
                        "," + " (expected, actual): " + expectedCount + ", " + recorded);
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Verification failed: " + e);
        }
    }
}
