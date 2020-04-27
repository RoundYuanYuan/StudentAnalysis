package com.hiveclient.util;

import com.hiveclient.bean.Student;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlConnect {
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://192.168.88.21:3306/final_design?useUnicode=true&characterEncoding=UTF-8";
    static final String USERNAME = "root";
    static final String PASSWORD = "123456";
    public static Connection getConn()
    {
        Connection conn=null;

        try{
            Class.forName(JDBC_DRIVER);
            conn= DriverManager.getConnection(DB_URL,USERNAME,PASSWORD);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return conn;
    }
    public static int insertStudent(Student student)
    {
        int res=0;
        Connection conn=getConn();
        String sql ="insert into t_student values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement) conn.prepareStatement(sql);
            pstmt.setInt(1, student.getId());
            pstmt.setInt(2,student.getGender());
            pstmt.setInt(3,student.getEmpstate());
            pstmt.setInt(4,student.getEducation());
            pstmt.setInt(5,student.getBiogland1());
            pstmt.setInt(6,student.getBiogland2());
            pstmt.setInt(7,student.getMajor1());
            pstmt.setInt(8,student.getMajor2());
            pstmt.setInt(9,student.getPrevious1());
            pstmt.setInt(10,student.getPrevious2());
            pstmt.setInt(11,student.getGrade());
            pstmt.setInt(12,student.getEnlevel());
            pstmt.setInt(13,student.getWorkplace1());
            pstmt.setInt(14,student.getWorkplace2());
            pstmt.setInt(15,student.getSalary());
            pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return res;
    }
}
