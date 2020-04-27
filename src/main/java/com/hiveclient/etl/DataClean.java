package com.hiveclient.etl;

import com.hiveclient.bean.Student;
import com.hiveclient.blp.Salary2num;
import com.hiveclient.util.HiveConnect;
import com.hiveclient.util.MysqlConnect;

import java.io.*;
import java.sql.*;
import java.util.Iterator;
import java.util.Properties;

public class DataClean {
    static String baseURI = "D:/SpringProject/hiveclient/src/main/resources/";
    private static String genderPath = "attributeMapping/gender.properties";
    private static String empstatePath = "attributeMapping/empstate.properties";
    private static String educationPath = "attributeMapping/education.properties";
    private static String provincePath = "attributeMapping/province.properties";
    private static String cityPath = "attributeMapping/city.properties";
    private static String major1Path = "attributeMapping/major1.properties";
    private static String major2Path = "attributeMapping/major2.properties";
    private static String previous1Path = "attributeMapping/previous1.properties";
    private static String previous2Path = "attributeMapping/previous2.properties";
    private static String gradePath = "attributeMapping/grade.properties";
    private static String enlevelPath = "attributeMapping/enlevel.properties";

    public static int getProperties(String propertiesPath, String str) {
        propertiesPath = baseURI + propertiesPath;
        Properties properties = new Properties();
        try {
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(propertiesPath));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, "UTF-8"));
            properties.load(bufferedReader);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Integer.valueOf((String) properties.get(str));
    }

    public void deleteDuplication() {
        Statement statement = HiveConnect.getStatement();
        String sql = "insert overwrite table t_dd_student select t.gender,\n" +
                "t.empstate,\n" +
                "t.education,\n" +
                "t.biogland1,\n" +
                "t.biogland2,\n" +
                "t.major1,\n" +
                "t.major2,\n" +
                "t.previous1,\n" +
                "t.previous2,\n" +
                "t.grade,\n" +
                "t.enlevel,\n" +
                "t.workplace1,\n" +
                "t.workplace2,\n" +
                "t.salary from (select *,row_number() over (partition by id order by id desc) num from t_pre_student) t where t.num=1";
        System.out.println("Running: " + sql);
        try {
            boolean execute = statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void dataTransfer() {
        String sql = "select * from t_original";
        Statement statement = HiveConnect.getStatement();
        try {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                Student student = new Student();
                student.setId(resultSet.getInt(1));
                student.setGender(getProperties(genderPath, resultSet.getString(2)));
                student.setEmpstate(getProperties(empstatePath, resultSet.getString(3)));
                student.setEducation(getProperties(educationPath, resultSet.getString(4)));
                student.setBiogland1(getProperties(provincePath, resultSet.getString(5)));
                student.setBiogland2(getProperties(cityPath, resultSet.getString(6)));
                student.setMajor1(getProperties(major1Path, resultSet.getString(7)));
                student.setMajor2(getProperties(major2Path, resultSet.getString(8)));
                student.setPrevious1(getProperties(previous1Path, resultSet.getString(9)));
                student.setPrevious2(getProperties(previous2Path, resultSet.getString(10)));
                student.setGrade(getProperties(gradePath, resultSet.getString(11)));
                student.setEnlevel(getProperties(enlevelPath, resultSet.getString(12)));
                student.setWorkplace1(getProperties(provincePath, resultSet.getString(13)));
                student.setWorkplace2(getProperties(cityPath, resultSet.getString(14)));
                student.setSalary(Salary2num.salary2Num(resultSet.getString(15)));
                MysqlConnect.insertStudent(student);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Student getStudentByID(int id, String tableName) {
        Student student = new Student();
        Statement statement = HiveConnect.getStatement();
        String sql = "select * from " + tableName + " where id=" + id;
        System.out.println("Running" + sql);
        try {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                student.setId(resultSet.getInt(1));
                student.setGender(resultSet.getInt(3));
                student.setEmpstate(resultSet.getInt(4));
                student.setEducation(resultSet.getInt(5));
                student.setBiogland1(resultSet.getInt(6));
                student.setBiogland2(resultSet.getInt(7));
                student.setWorkplace1(resultSet.getInt(8));
                student.setWorkplace2(resultSet.getInt(9));
                student.setMajor1(resultSet.getInt(10));
                student.setMajor2(resultSet.getInt(11));
                student.setPrevious1(resultSet.getInt(12));
                student.setPrevious2(resultSet.getInt(13));
                student.setGrade(resultSet.getInt(14));
                student.setEnlevel(resultSet.getInt(15));
                student.setSalary(resultSet.getInt(16));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return student;
    }
}
