package com.test;

import com.test.LogReader.LogManager;

import java.sql.SQLException;

public class Main {

    public static void main(String[] args) {
	// write your code here
        try {
            new LogManager().readLog();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
