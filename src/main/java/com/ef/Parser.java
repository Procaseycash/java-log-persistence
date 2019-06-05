package com.ef;

import com.ef.LogReader.LogManager;

import java.sql.SQLException;

public class Parser {

    public static void main(String[] args) {
	// write your code here
        try {
            new LogManager().readLog();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
