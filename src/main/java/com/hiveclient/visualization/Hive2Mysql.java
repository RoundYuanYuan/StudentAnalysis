package com.hiveclient.visualization;

import com.hiveclient.util.HiveConnect;
import com.hiveclient.util.MysqlConnect;

import java.sql.*;

public class Hive2Mysql {
    public static void main(String[] args) throws SQLException {
        cleanTable();
        getHeatMap();
        getGender();
        getRed();
        getFlyMap();
        getEmpstate();
//        test();
    }
    public static int cleanTable()
    {
        int flag=0;
        Connection connection=MysqlConnect.getConn();
        String [] tables={"t_empstate","t_gender","t_migrate","t_position","t_red"};
        for (int i=0;i<tables.length;i++)
        {
            String cleanSql="TRUNCATE TABLE "+tables[i];
            try {
                PreparedStatement preparedStatement = connection.prepareStatement(cleanSql);
                preparedStatement.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        flag=1;
        return flag;
    }

//    public static void  test() throws SQLException {
//        Statement statement = HiveConnect.getStatement();
//        String sql="select * from finaldesign.t_student";
//        ResultSet resultSet = statement.executeQuery(sql);
//        while (resultSet.next()){
//            String string = resultSet.getString(2);
//            System.out.println(string);
//        }
//    }


    public static void getHeatMap()
    {
        Connection conn = MysqlConnect.getConn();
        PreparedStatement pstmt;
        Statement statement = HiveConnect.getStatement();
        String hivesql="select workplace2,count(*) from t_original group by workplace2";
        String mysql="insert into t_position values(?,?,?)";

        try {
            pstmt =conn.prepareStatement(mysql);
            ResultSet resultSet = statement.executeQuery(hivesql);
            while (resultSet.next()) {
                String city = resultSet.getString(1);
                String count = resultSet.getString(2);
                String[] latlng = getPosition(city);
                pstmt.setString(1,latlng[0]);
                pstmt.setString(2,latlng[1]);
                pstmt.setString(3,count+"0");
                pstmt.execute();
            }
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void getGender() {
        Connection conn = MysqlConnect.getConn();
        String mysql = "insert into t_gender values(?,?)";
        PreparedStatement pstmt;
        Statement statement = HiveConnect.getStatement();
        String sql = "select gender,count(*) from t_original group by gender";
        try {
            pstmt = conn.prepareStatement(mysql);
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String string = resultSet.getString(1);
                String[] split = string.split("\\.");
                pstmt.setString(1,split[1]);
                pstmt.setString(2, resultSet.getString(2));
                pstmt.execute();
            }
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void getRed() {
        Connection conn = MysqlConnect.getConn();
        String mysql = "insert into t_red values(?,?)";
        PreparedStatement pstmt;
        Statement statement = HiveConnect.getStatement();
        String sql = "SELECT \n" +
                "CASE WHEN salary <= 5000 THEN '5000' \n" +
                "WHEN salary > 5000 AND salary <= 10000  THEN '10000' \n" +
                "WHEN salary > 10000 AND salary <= 15000  THEN '15000' \n" +
                "WHEN salary > 15000 AND salary <= 20000 THEN '20000' \n" +
                "ELSE NULL END salary_class,\n" +
                "COUNT(*)  FROM t_original \n" +
                "GROUP BY \n" +
                "CASE WHEN salary <= 5000 THEN '5000' \n" +
                "WHEN salary > 5000 AND salary <= 10000  THEN '10000' \n" +
                "WHEN salary > 10000 AND salary <= 15000  THEN '15000' \n" +
                "WHEN salary > 15000 AND salary <= 20000 THEN '20000' \n" +
                "ELSE NULL END";
        try {
            pstmt = conn.prepareStatement(mysql);
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                pstmt.setString(1, resultSet.getString(1));
                pstmt.setString(2, resultSet.getString(2));
                pstmt.execute();
            }
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static String[] getPosition(String name){
        String lat="";
        String lng="";
        Connection conn = MysqlConnect.getConn();
        String getPosition ="select lat,lng from t_latlng where name=?";
        PreparedStatement pstmt;
        try{
            pstmt =conn.prepareStatement(getPosition);
            pstmt.setString(1,name);
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()){
                lat=resultSet.getString(1);
                lng=resultSet.getString(2);
            }
        }catch (SQLException e)
        {
            e.printStackTrace();
        }
        String res[]={lat,lng};
        return res;
    }
    public static void getFlyMap()
    {
        Connection conn = MysqlConnect.getConn();
        String mysql ="insert into t_migrate values(?,?,?)";
        PreparedStatement pstmt;
        Statement statement = HiveConnect.getStatement();
        String sql="select biogland2,workplace2,count(*) from t_original group by biogland2,workplace2";
        try {
            pstmt =conn.prepareStatement(mysql);
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                pstmt.setString(1, resultSet.getString(2));
                pstmt.setString(2, resultSet.getString(1));
                pstmt.setInt(3,resultSet.getInt(3));
                pstmt.execute();
            }
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
    public static void getEmpstate() {
        Connection conn = MysqlConnect.getConn();
        String mysql = "insert into t_empstate values(?,?)";
        PreparedStatement pstmt;
        Statement statement = HiveConnect.getStatement();
        String sql = "select empstate,count(*) from t_original group by empstate";
        try {
            pstmt = conn.prepareStatement(mysql);
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String string = resultSet.getString(1);
                String[] split = string.split("\\.");
                pstmt.setString(1,split[1]);
                pstmt.setString(2, resultSet.getString(2));
                pstmt.execute();
            }
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
