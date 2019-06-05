package com.ef;

import com.ef.LogReader.LogManager;

import java.sql.SQLException;

public class Parser {

    public static void main(String[] args) {
        try {
            new LogManager(args);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Error Message:" + e.getMessage());
        }

    }
}
