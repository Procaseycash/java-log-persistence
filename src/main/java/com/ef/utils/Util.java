package com.ef.utils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Util {
    private static Connection connection = null;

    /**
     * This is used to initiate connection to db
     */
    private static Connection initConnection() {
        Connection con = null;
        try {
            Properties pros = getProperties();
            String url = pros.getProperty("url");
            String user = pros.getProperty("user");
            String password = pros.getProperty("password");
            con = DriverManager.getConnection(url, user, password);
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        connection = con;
        return con;
    }

    private static void dropTableAccessLog() throws SQLException {
        connection.createStatement().executeUpdate("DROP TABLE IF EXISTS access_logs");
    }

    public static void createAccessLogTable() {
        try {
            dropTableAccessLog(); // drop table
            String sql = "CREATE TABLE  IF NOT EXISTS access_logs " +
                    "(id INTEGER not NULL AUTO_INCREMENT, " +
                    " created_at DATETIME, " +
                    " ip_address VARCHAR(255), " +
                    " request VARCHAR(255), " +
                    " request_status INTEGER, " +
                    " user_agent VARCHAR(255), " +
                    " PRIMARY KEY ( id ))";
            connection.createStatement().executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println("Error Message: " + e.getMessage());
        }
    }

    /**
     * This is used to get app properties
     *
     * @return
     * @throws IOException
     */
    private static Properties getProperties() throws IOException {
        InputStream f = ClassLoader.getSystemResourceAsStream("app.properties");
        Properties pros = new Properties();
        pros.load(f);
        return pros;
    }

    public static Connection getConnection() {
        return connection != null ? connection : Util.initConnection();
    }
}
