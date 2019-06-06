package com.ef.LogReader;

import com.ef.enums.DurationEnum;
import com.ef.utils.Util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/***
 * Author: Kazeem Olanipekun
 * File Log Manager for reading and printing numbers of times an IP was repeated.
 */
public class LogManager {
    private String startDate = null;
    private String duration = null;
    private String accessLog = null;
    private int threshold = 0;

    /**
     * This is used to read log from file for db storage
     */
    public LogManager(String[] args) {
        if (Util.getConnection() == null) {
            System.out.println("Connection failed");
            System.exit(0);
        }
        run(args);
    }

    /**
     * Process Argument parser
     *
     * @param args
     */
    private void argumentParser(String[] args) {
        for (String arg : args) {
            if (arg.contains("--startDate")) {
                startDate = arg.split("=")[1];
            } else if (arg.contains("--duration")) {
                duration = arg.split("=")[1];
            } else if (arg.contains("--threshold")) {
                threshold = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.contains("--accesslog")) {
                accessLog = arg.split("=")[1];
            }
        }
        if (startDate == null || duration == null || threshold < 0 || accessLog == null) {
            System.out.println("Wrong Argument passed");
            System.exit(0);
        }
    }

    /**
     * This process argument and output expected result
     */
    private void run(String[] args) {
        try {
            Util.createAccessLogTable(); // initiate Table creation
            argumentParser(args); // parse argument
            loadAccessLogToDb(); // load access log to table
            analyseInputWithThreshold(); // process query request from args
        } catch (SQLException | IOException e) {
            System.out.println("Error Message:" + e.getMessage());
        }
    }


    /**
     * This is used to load access log file into db for storage
     */
    private void loadAccessLogToDb() throws SQLException, IOException {
        String sql = "INSERT INTO access_logs (created_at, ip_address, request, request_status, user_agent) VALUES (?, ?, ?, ?, ?)";
        Util.getConnection().setAutoCommit(false);
        PreparedStatement ps = Util.getConnection().prepareStatement(sql);
        Files.lines(Paths.get(accessLog)).parallel().map(str -> str.split("\\|")).forEach(colStrings -> {
            try {
                int i = 1;
                for (String colString : colStrings) {
                    if (i == 1) ps.setTimestamp(i, Timestamp.valueOf(colString));
                    else ps.setString(i, colString);
                    i++;
                }
                ps.addBatch();
            } catch (SQLException e) {
                System.out.println("Error Message:" + e.getMessage());
                System.exit(0);
            }
        });
        ps.executeBatch();
        Util.getConnection().commit();
    }


    /**
     * This is used to log output result to console once command argument is proccessed
     */
    private void analyseInputWithThreshold() throws IllegalArgumentException, SQLException {
        String sDate = startDate.replace('.', 'T');
        startDate = startDate.replace('.', ' ');
        String endDate = "";
        if (DurationEnum.valueOf(duration.toUpperCase()) == DurationEnum.HOURLY) {
            endDate = Timestamp.valueOf(LocalDateTime.parse(sDate).plusHours(1)).toString();
        } else if (DurationEnum.valueOf(duration.toUpperCase()) == DurationEnum.DAILY) {
            endDate = Timestamp.valueOf(LocalDateTime.parse(sDate).plusHours(23).plusMinutes(59).plusSeconds(59)).toString();
        }
        String query = "SELECT ip_address, SUM(CASE WHEN ip_address IS NOT NULL THEN 1 else 0 END) AS ip_record " +
                " FROM access_logs " +
                " WHERE created_at >= ? AND created_at <= ? " +
                " GROUP BY ip_address HAVING count(ip_address) >= ?";
        PreparedStatement ps = Util.getConnection().prepareStatement(query);
        ps.setTimestamp(1, Timestamp.valueOf(startDate));
        ps.setTimestamp(2, Timestamp.valueOf(endDate));
        ps.setInt(3, threshold);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            System.out.printf("If you open the log file, %s has %d or more requests between %s and %s\n", rs.getString("ip_address"), rs.getInt("ip_record"), startDate, endDate);
        }
        rs.close();
        Util.getConnection().close();
    }


}

