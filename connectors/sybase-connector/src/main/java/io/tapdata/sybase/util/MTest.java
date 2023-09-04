package io.tapdata.sybase.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;

/**
 * @author GavinXiao
 * @description MTest create by Gavin
 * @create 2023/8/2 14:13
 **/
public class MTest {
    public static final String INSERT_POC_TEST_SQL = "insert into tester.poc_test_no_txt_not_null_s (" +
            "char_col," +
            "datetime_col," +
            "decimal_col," +
            "float_col," +
            "int_col," +
            "money_col," +
            "numeric_col," +
            "nvarchar_col," +
            "smalldatetime_col," +
            "smallint_col," +
            "sysname_col," +
            "tinyint_col," +
            "varchar_col" +
            ") " +
            "values (?,?,?,?,?,?,?,?,?,?,?,?,? )";

    public static void insert(String sql, Connection connection) throws Exception {
        PreparedStatement p = connection.prepareStatement(sql);
        int index = 1;
        String rad = Code.rad();
        p.setString(index++, new String((rad).getBytes("utf-8"), "big5"));
        p.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
        p.setDouble(index++, 2.36);
        p.setDouble(index++, 2.36);
        p.setInt(index++, 8);
        p.setDouble(index++, 3.33);
        p.setDouble(index++, 4.33);
        p.setString(index++, new String((rad + "這個是一段正體字文字，我要把它從cp850轉成utf-8").getBytes("big5-hkscs"), "cp850")); // 使用setBytes方法设置二?制?据
        p.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
        p.setInt(index++, 3);
        p.setString(index++, new String((rad + "Fdsd").getBytes("utf-8"), "big5"));
//        p.setString(index++, new String(("" +
//                "，我要把它從cp850轉成utf-8").getBytes("big5-hkscs"), "cp850")); // 使用setBytes方法设置二?制?据
        p.setInt(index++, 3);
        p.setString(index++, new String((rad + "這個是一段正體字文字，我要把它從cp850轉成utf-8").getBytes("big5-hkscs"), "cp850")); // 使用setBytes方法设置二?制?据
        p.executeUpdate();
    }

    public static void main(String[] args) {

        try {

            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            Connection conn = DriverManager.getConnection("jdbc:jtds:sybase://139.198.105.8:45000/testdb", "tester", "guest1234");
            Statement statement = conn.createStatement();
            //insertOne("INSERT INTO tester.poc_test (varchar_col) VALUES (?)", conn);
            //conn.setAutoCommit(false);
            for (int i = 0; i < 2000000; i++) {
                insert(INSERT_POC_TEST_SQL, conn);
            }
            conn.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
