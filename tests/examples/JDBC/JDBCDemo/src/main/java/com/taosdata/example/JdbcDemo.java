package com.taosdata.example;

import com.taosdata.jdbc.TSDBPreparedStatement;
import java.sql.*;
import java.util.Random;
import java.util.ArrayList;
import java.util.Properties;

public class JdbcDemo {
    private static String host;
    private static final String dbName = "test";
    private static final String tbName = "weather";
    private Connection connection;

    public static void main(String[] args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            if ("-host".equalsIgnoreCase(args[i]) && i < args.length - 1)
                host = args[++i];
        }
        if (host == null) {
            printHelp();
        }
        JdbcDemo demo = new JdbcDemo();
        demo.init();
        demo.createDatabase();
        demo.useDatabase();
        demo.dropTable();
        demo.createTable();
        demo.insert();
        demo.select();
        demo.dropTable();
        demo.close();
    }

    private void init() {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        // get connection
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty("charset", "UTF-8");
            properties.setProperty("locale", "en_US.UTF-8");
            properties.setProperty("timezone", "UTC-8");
            System.out.println("get connection starting...");
            connection = DriverManager.getConnection(url, properties);
            if (connection != null)
                System.out.println("[ OK ] Connection established.");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void createDatabase() {
        String sql = "create database if not exists " + dbName;
        exuete(sql);
    }

    private void useDatabase() {
        String sql = "use " + dbName;
        exuete(sql);
    }

    private void dropTable() {
        final String sql = "drop table if exists " + dbName + "." + tbName + "";
        exuete(sql);
    }

    private void createTable() {
        
        StringBuilder sb = new StringBuilder();
        sb.append("create table if not exists " + dbName + "." + tbName + " (ts timestamp, ");
        for(int i = 0; i < 20; i++) {
            sb.append("i" + i + " int, ");
        }
        for(int i = 0; i < 20; i++) {
            sb.append("d" + i + " double");
            if(i != 19) {
                sb.append(", ");
            }else {
                sb.append(")");
            }
        }
        System.out.println(sb.toString());
        exuete(sb.toString());
    }

    private void insert() {        
        StringBuilder sb = new StringBuilder();
        Random rand = new Random();
        sb.append("insert into " + dbName + "." + tbName + " values");
        for(int row = 0; row < 20; row++) {
            sb.append("(now+" + row + "s,");
            for(int i = 0; i < 20; i++) {
                sb.append(rand.nextInt(1000) + ", ");
            }
            for(int i = 0; i < 20; i++) {
                sb.append(rand.nextDouble());
                if(i != 19) {
                    sb.append(", ");
                }
            }
            sb.append(")");
            // System.out.println("insert sql is" + sb.toString());
        }
        exuete(sb.toString());
    }

    private void dataBindingInsert() throws SQLException {
        TSDBPreparedStatement s = (TSDBPreparedStatement) connection.prepareStatement("insert into ? values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        s.setTableName(dbName + "." + tbName);
        
        int batches = 100;
        Random rand = new Random();

        for (int batch = 0; batch < batches; batch++) {

            ArrayList<Long> ts = new ArrayList<Long>();
            for (int i = 0; i < 60; i++) {
                ts.add(System.currentTimeMillis() + batch * 60 + i);
            }
            s.setTimestamp(0, ts);

            for(int i = 0; i < 20; i++) {

                ArrayList<Integer> tmp = new ArrayList<>();
                for(int j = 0; j < 60; j++) {
                    tmp.add(rand.nextInt(100));
                }
                s.setInt(i, tmp);
            }

            for(int i = 20; i < 40; i++) {

                ArrayList<Double> tmp = new ArrayList<>();
                for(int j = 0; j < 60; j++) {
                    tmp.add(rand.nextDouble());
                }
                s.setDouble(i, tmp);
            }
            
            s.columnDataAddBatch();
            long start = System.currentTimeMillis();
            s.columnDataExecuteBatch();
            long end = System.currentTimeMillis();

            System.out.println("start:" + start + ", end: " + end + ", cost: " + (end - start));
            s.columnDataCloseBatch();
            
        }

        
        
    }

    private void select() {
        final String sql = "select * from "+ dbName + "." + tbName;
        executeQuery(sql);
    }

    private void close() {
        try {
            if (connection != null) {
                this.connection.close();
                System.out.println("connection closed.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /************************************************************************/

    private void executeQuery(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            printSql(sql, true, (end - start));
            printResult(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void printResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnLabel = metaData.getColumnLabel(i);
                String value = resultSet.getString(i);
                System.out.printf("%s: %s\t", columnLabel, value);
            }
            System.out.println();
        }
    }


    private void printSql(String sql, boolean succeed, long cost) {
        System.out.println("[ " + (succeed ? "ERROR!" : "OK") + " ] time cost: " + cost + " ms");
    }

    private void exuete(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            boolean execute = statement.execute(sql);
            long end = System.currentTimeMillis();
            printSql(sql, execute, (end - start));
        } catch (SQLException e) {
            e.printStackTrace();

        }
    }

    private static void printHelp() {
        System.out.println("Usage: java -jar JDBCDemo.jar -host <hostname>");
        System.exit(0);
    }


}
