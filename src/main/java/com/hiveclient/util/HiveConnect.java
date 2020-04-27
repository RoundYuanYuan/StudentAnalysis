package com.hiveclient.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveConnect {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static Statement stmt=null;
    public static Statement getStatement()
    {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = null;
        try {
            con = DriverManager.getConnection("jdbc:hive2://192.168.88.21:10000/finaldesign");
            stmt = con.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return stmt;
    }
}
