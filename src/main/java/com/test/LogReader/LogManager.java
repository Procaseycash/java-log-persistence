package com.test.LogReader;

import com.test.enums.DurationEnum;
import com.test.utils.Util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Properties;

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
    public void readLog() throws SQLException {
        Util.setConnection(Util.initConnection());
        if (Util.getConnection() == null) throw new SQLException("Connection failed");
        Util.createAccessLogTable(); // initiate Table creation
        try {
            Properties props = Util.getProperties();
            String fileString = new String(Files.readAllBytes(Paths.get(props.getProperty("filePath"))));
            loadAccessLogToDb(fileString.split("\\n")); // initiate db storage of access log
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * This is used to load access log file into db for storage
     *
     * @param fileStrings
     * @throws SQLException
     */
    private void loadAccessLogToDb(String[] fileStrings) throws SQLException {
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
        analyseInputWithThreshold("2017-01-01.00:00:00", "daily", 200);
    }


    /**
     * This is used to log output result to console once command argument is proccessed
     */
    private void analyseInputWithThreshold(String startDate, String duration, int threshold) throws IllegalArgumentException, SQLException {
        String sDate = startDate.replace('.', 'T');
        startDate = startDate.replace('.', ' ');
        String endDate = "";
        System.out.println("dd=" + DurationEnum.valueOf(duration.toUpperCase()));
        if (DurationEnum.valueOf(duration.toUpperCase()) == DurationEnum.HOURLY) {
            endDate = Timestamp.valueOf(LocalDateTime.parse(sDate).plusHours(1)).toString();
        } else if (DurationEnum.valueOf(duration.toUpperCase()) == DurationEnum.DAILY) {
            endDate = Timestamp.valueOf(LocalDateTime.parse(sDate).plusHours(23).plusMinutes(59).plusSeconds(59)).toString();
        }
        System.out.println(startDate + '-' + endDate);
        String query = "SELECT ip_address, SUM(CASE WHEN ip_address IS NOT NULL THEN 1 else 0 END) AS ip_record " +
                " FROM access_logs " +
                " WHERE created_at >= ? AND created_at <= ? " +
                " GROUP BY ip_address HAVING count(ip_address) >= ?";
        PreparedStatement ps = Util.getConnection().prepareStatement(query);
        ps.setTimestamp(1, Timestamp.valueOf(startDate));
        ps.setTimestamp(2, Timestamp.valueOf(endDate));
        ps.setInt(3, threshold);
        System.out.println("query = " + query);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println("Ip Address = " + rs.getString("ip_address") + " | " + rs.getInt("ip_record"));
        }
        rs.close();
        Util.getConnection().close();
    }

    /**
     * This is used to store output result in db
     */
    public void StoreLogResult() {

    }

}

