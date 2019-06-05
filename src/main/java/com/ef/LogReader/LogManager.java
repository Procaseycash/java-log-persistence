package com.ef.LogReader;

import com.ef.enums.DurationEnum;
import com.ef.utils.Util;
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

    /**
     * This is used to read log from file for db storage
     *
     * @throws SQLException
     */
    public LogManager(String[] args) throws SQLException {
        Util.setConnection(Util.initConnection());
        if (Util.getConnection() == null) throw new SQLException("Connection failed");
        Util.createAccessLogTable(); // initiate Table creation
        try {
            this.run(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error Message: " + e.getMessage());
        }
    }
    /**
     * This process argument and output expected result
     */
    private void run(String[] args) throws Exception {
        String startDate = null;
        String duration = null;
        String accessLog = null;
        int threshold = 0;
        for (String arg : args) {
//            System.out.println("arg=" + arg);
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
        try {
            System.out.println("StartDate=" + startDate + "  duration=" + duration + " threshold=" + threshold + " accessLog=" + accessLog);
            if (startDate == null || duration == null || threshold < 0 || accessLog == null)
                throw new Exception("Wrong Argument passed");
            String fileString = new String(Files.readAllBytes(Paths.get(accessLog)));
            loadAccessLogToDb(fileString.split("\\n"), startDate, duration, threshold); // initiate db storage of access log
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * This is used to load access log file into db for storage
     *
     * @param fileStrings
     * @throws SQLException
     */
    private void loadAccessLogToDb(String[] fileStrings, String startDate, String duration, int threshold) throws SQLException {
        String sql = "INSERT INTO access_logs (created_at, ip_address, request, request_status, user_agent) VALUES (?, ?, ?, ?, ?)";
        Util.getConnection().setAutoCommit(false);
        PreparedStatement ps = Util.getConnection().prepareStatement(sql);
        for (String fileString : fileStrings) {
            String[] colStrings = fileString.split("\\|");
            int i = 1;
            for (String colString : colStrings) {
                if (i == 1) ps.setTimestamp(i, Timestamp.valueOf(colString));
                else ps.setString(i, colString);
                i++;
            }
            ps.addBatch();
        }
        ps.executeBatch();
        Util.getConnection().commit();
        analyseInputWithThreshold(startDate, duration, threshold); // This is used to run the analysis carried out
    }


    /**
     * This is used to log output result to console once command argument is proccessed
     */
    private void analyseInputWithThreshold(String startDate, String duration, int threshold) throws IllegalArgumentException, SQLException {
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

