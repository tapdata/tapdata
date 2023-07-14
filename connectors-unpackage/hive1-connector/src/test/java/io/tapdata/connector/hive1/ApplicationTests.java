//package io.tapdata.connector.hive1;
//
//import org.junit.jupiter.api.Test;
//
//import javax.naming.Name;
//import java.sql.*;
//
//class ApplicationTests {
//    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
//
//    @Test
//    void contextLoads() throws ClassNotFoundException, SQLException {
//        Class.forName(driverName);
//        Connection connection = DriverManager.getConnection("jdbc:hive2://192.168.1.179:10000/default", "root", "");
////        if (connection.isValid(5)) {
////            System.out.println("success");
////        } else {
////            System.out.println("fail");
////        }
//        Statement statement = connection.createStatement();
//        ResultSet resultSet = statement.executeQuery("select * from student");
//        while (resultSet.next()) {
//            String string = resultSet.getString(1);
//            String string1 = resultSet.getString(2);
//            System.out.println("string:"+string+",string1:"+string1+",resultSet:"+resultSet);
//        }
//        if (connection.isValid(5)) {
//            System.out.println("success");
//        }else{
//            System.out.println("fail");
//        }
//        System.out.println("无值");
//        try {
//            Thread.sleep(50 * 1000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Test
//    public void testTime() {
//        Date date = new Date(1660230064L);
//        System.out.println(date);
//    }
//
//    @Test
//    public void test2() {
//        String str = "name";
//        String[] split = str.split(",");
//        for (String s : split) {
//            System.out.println(s);
//        }
//    }
//
//}
