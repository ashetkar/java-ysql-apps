# How to run

- Running the programm without any arguments will print its usage.

    ```
    $ YBDB_PATH=<path/to/YugabyteDB/installation/dir> mvn compile exec:java -Dexec.mainClass="com.yugabyte.ysql.TestApp"
    CLI arguments:
            "1"        - Run simpleApp()
            "2"        - Run performanceApp() in single thread with JDBC Smart driver
            "2 pgjdbc" - Run performanceApp() in single thread with PGJDBC driver
            "3"        - Run performanceApp() in multiple threads with JDBC Smart driver
            "3 pgjdbc" - Run performanceApp() in multiple threads with PGJDBC driver
    ```

- Below will run `performanceApp()` with additional argument 'pgjdbc':

    ```
    YBDB_PATH=<path/to/YugabyteDB/installation/dir> mvn compile exec:java -Dexec.mainClass="com.yugabyte.ysql.TestApp" -Dexec.args="2 pgjdbc"
    ```

- More apps may follow.
