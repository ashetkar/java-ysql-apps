package com.yugabyte.ysql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class TestApp {

    private static int numConnectionsPerThread = 6;
    private static int numThreads = 25;
    public static final String path = System.getenv("YBDB_PATH");
    private static boolean useVanillaPGJDBC = false; // CHANGE pom.xml to include appropriate driver if you change this

    public static void main(String[] args) throws SQLException, InterruptedException {
        if (args == null || args.length < 1) {
            System.out.println("Usage: TestApp [1|2 [pgjdbc]|3 [pgjdbc]]");
            return;
        }
        switch (args[0]) {
            case "1":
                simpleApp();
                break;
            case "2":
                useVanillaPGJDBC = args.length > 1 && "pgjdbc".equalsIgnoreCase(args[1]);
                performanceApp(true);
                break;
            case "3":
                useVanillaPGJDBC = args.length > 1 && "pgjdbc".equalsIgnoreCase(args[1]);
                performanceApp(false);
                break;
            default:
                System.out.println("Usage: TestApp [1|2 [pgjdbc]|3 [pgjdbc]]");
        }
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
        // &loggerLevel=DEBUG";
        String baseUrl = "jdbc:" + (useVanillaPGJDBC ? "postgresql" : "yugabytedb")
                + "://localhost:5433/yugabyte?load-balance=true&yb-servers-refresh-interval=300";
        System.out.println("Running perf tests for " + (serial ? "serial" : "concurrent")
                + " connections with " + (useVanillaPGJDBC ? "PGJDBC" : "Smart") + " driver, with url: " + baseUrl);
        int total = numThreads * numConnectionsPerThread;
        Map<String, Integer> expected1;
        if (useVanillaPGJDBC) {
            expected1 = expectedInput(total, 0, 0);
        } else {
            expected1 = expectedInput(total/3 + 1, total/3, total/3);
        }
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
        int iterations = 10;
        long[] times = new long[iterations];
        try {
            for (int iteration = 0; iteration < iterations; iteration++) {
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

                StringBuilder msg = new StringBuilder();
                for (Map.Entry<String, Integer> e : expected1.entrySet()) {
                    msg.append(verifyOn(e.getKey(), e.getValue(), controlHost)).append(", ");
                }
                System.out.println(msg);

//                System.out.println("Closing connections ...");
                for (int i = 0; i < numThreads; i++) {
                    for (int j = 0; j < numConnectionsPerThread; j++) {
                        connections[i][j].close();
                    }
                }
            }
            long avg = 0;
            for (int i = 1; i < iterations; i++) { // skip the first one
                avg += times[i];
            }
            avg = avg / (iterations-1);
            System.out.println("Average time " + avg + " ms");

        } finally {
            executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 15);
        }
    }

    private static void testSingleThreadConnectionCreations(String url, Map<String,
            Integer> expected1, String controlHost) throws SQLException, InterruptedException {
        startYBDBCluster();
        System.out.println("Cluster started!");
        Thread.sleep(5000);
        int totalConnections = 150;
        int iterations = 10;
        long[] times = new long[iterations];
        try {
            for (int iteration = 0; iteration < iterations; iteration++) {
                Connection[] connections = new Connection[totalConnections];

    //            System.out.println("Creating " + totalConnections + " connections ...");
                long start = System.currentTimeMillis();
                for (int i = 0 ; i < totalConnections; i++) {
                    try {
                        connections[i] = DriverManager.getConnection(url, "yugabyte", "yugabyte");
                    } catch (SQLException e) {
                        System.out.println("[" + i + "] getConnection() failed: " + e);
                    }
                }
                long elapsed = System.currentTimeMillis() - start;
                System.out.println("[Iteration " + iteration + "] All " + totalConnections + " connections created in " + elapsed + " ms");
                times[iteration] = elapsed;

                StringBuilder msg = new StringBuilder("");
                for (Map.Entry<String, Integer> e : expected1.entrySet()) {
                    msg.append(verifyOn(e.getKey(), e.getValue(), controlHost)).append(", ");
                }
                System.out.print(msg);

//                System.out.println("Closing connections ...");
                for (int i = 0 ; i < totalConnections; i++) {
                    connections[i].close();
                }
            }
            long avg = 0;
            for (int i = 1; i < iterations; i++) { // skip the first one
                avg += times[i];
            }
            avg = avg / (iterations-1);
            System.out.println("Average time " + avg + " ms");
        } finally {
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

    public static String verifyOn(String server, int expectedCount, String multipurposeParam) {
        String msg = "";
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
            msg = server + " = " + (count.length - 1);
            // Server side validation
            if (expectedCount != (count.length - 1)) {
                throw new RuntimeException("Client backend processes did not match for " + server + ", " +
                        "(expected, actual): " + expectedCount + ", " + (count.length - 1));
            }
            // Client side validation
            if (useVanillaPGJDBC) {
                return msg;
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
        return msg;
    }
}
