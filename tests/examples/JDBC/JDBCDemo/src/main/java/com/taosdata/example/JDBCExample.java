package com.taosdata.example;

import java.sql.*;
import java.util.Random;
import org.apache.iotdb.jdbc.IoTDBSQLException;

public class JDBCExample {


    /**
     * Before executing a SQL statement with a Statement object, you need to create a Statement object using the createStatement() method of the Connection object.
     * After creating a Statement object, you can use its execute() method to execute a SQL statement
     * Finally, remember to close the 'statement' and 'connection' objects by using their close() method
     * For statements with query results, we can use the getResultSet() method of the Statement object to get the result set.
     */

    static int ts = 1592809176;
    
    public static void main(String[] args) throws SQLException {
        Connection connection = getConnection();
        if (connection == null) {
        System.out.println("get connection defeat");
        return;
        }
        Statement statement = connection.createStatement();
        //Create storage group
        try {
        statement.execute("SET STORAGE GROUP TO root.demo");
        }catch (SQLException e){
        System.out.println(e.getMessage());
        }


        //Show storage group
        statement.execute("SHOW STORAGE GROUP");
        outputResult(statement.getResultSet());

        //Create time series
        //Different data type has different encoding methods. Here use INT32 as an example
        

        //Show time series
        // statement.execute("SHOW TIMESERIES root.demo");
        // outputResult(statement.getResultSet());
        // //Show devices
        // statement.execute("SHOW DEVICES");
        // outputResult(statement.getResultSet());
        // //Count time series
        // statement.execute("COUNT TIMESERIES root");
        // outputResult(statement.getResultSet());
        // //Count nodes at the given level
        // statement.execute("COUNT NODES root LEVEL=3");
        // outputResult(statement.getResultSet());
        //Count timeseries group by each node at the given level
        // statement.execute("COUNT TIMESERIES root GROUP BY LEVEL=3");
        // outputResult(statement.getResultSet());
        
        

        try {
            for(int i = 0; i < 20; i++) {
                statement.execute("CREATE TIMESERIES root.demo.s" + i + " WITH DATATYPE=INT32,ENCODING=PLAIN;");    
            }
            for(int i = 20; i < 40; i++) {
                statement.execute("CREATE TIMESERIES root.demo.s" + i + " WITH DATATYPE=DOUBLE,ENCODING=PLAIN;");
            }        
        }catch (SQLException e){
            System.out.println(e.getMessage());
        }
        
        Random rand = new Random();
        //Execute insert statements in batch
        
        for (int batch = 0; batch < 100; batch++) {
            for (int i = 0; i < 60; i++){
                for (int j = 0; j < 40; j++) {
                    int tmpts = ts + batch * 60 + i;
                    if(j < 20) {
                        statement.addBatch("insert into root.demo(timestamp, s" + j + ") values(" + tmpts + ", " + rand.nextInt(100) + ")");
                    } else {
                        statement.addBatch("insert into root.demo(timestamp, s" + j + ") values(" + tmpts + ", " + rand.nextDouble()+ ")");
                    }
                }
            }

            long start = System.nanoTime();
            statement.executeBatch();
            long end = System.nanoTime();
            System.out.println("start: " + start + ",end: " + end + ", cost: " + (end - start) + " ns");
            statement.clearBatch();
        }


        
        // statement.execute("delete storage group root.demo");
        

        //Full query statement
        // String sql = "select * from root.demo";
        // ResultSet resultSet = statement.executeQuery(sql);
        // System.out.println("sql: " + sql);
        // outputResult(resultSet);

        // //Exact query statement
        // sql = "select s0 from root.demo where time = 4;";
        // resultSet= statement.executeQuery(sql);
        // System.out.println("sql: " + sql);
        // outputResult(resultSet);

        // //Time range query
        // sql = "select s0 from root.demo where time >= 2 and time < 5;";
        // resultSet = statement.executeQuery(sql);
        // System.out.println("sql: " + sql);
        // outputResult(resultSet);

        // //Aggregate query
        // sql = "select count(s0) from root.demo;";
        // resultSet = statement.executeQuery(sql);
        // System.out.println("sql: " + sql);
        // outputResult(resultSet);

        //Delete time series
        

        //close connection
        statement.close();
        connection.close();
    }

    public static Connection getConnection() {
        // JDBC driver name and database URL
        String driver = "org.apache.iotdb.jdbc.IoTDBDriver";
        String url = "jdbc:iotdb://127.0.0.1:6667/";

        // Database credentials
        String username = "root";
        String password = "root";

        Connection connection = null;
        try {
        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
        e.printStackTrace();
        } catch (SQLException e) {
        e.printStackTrace();
        }
        return connection;
    }

    /**
     * This is an example of outputting the results in the ResultSet
     */
    private static void outputResult(ResultSet resultSet) throws SQLException {
        if (resultSet != null) {
        System.out.println("--------------------------");
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            System.out.print(metaData.getColumnLabel(i + 1) + " ");
        }
        System.out.println();
        while (resultSet.next()) {
            for (int i = 1; ; i++) {
                System.out.print(resultSet.getString(i));
                if (i < columnCount) {
                    System.out.print(", ");
                } else {
                    System.out.println();
                    break;
                }
            }
        }
        System.out.println("--------------------------\n");
        }
    }
}