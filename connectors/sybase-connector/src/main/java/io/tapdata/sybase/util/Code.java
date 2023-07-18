package io.tapdata.sybase.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.StringJoiner;

/**
 * @author GavinXiao
 * @description Code create by Gavin
 * @create 2023/7/14 15:36
 **/
public class Code {
    public static final int STREAM_READ_WARN = -100001;

    public static final String MACHE_REGEX = "(.*)(CHAR|char|TEXT|text)(.*)";

    public static void select(String sql, Statement statement) throws Exception {
        ResultSet rs = statement.executeQuery(sql);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            StringJoiner joiner = new StringJoiner(" --- ");
            for (int i = 1; i < columnCount + 1; i++) {
                String columnTypeName = metaData.getColumnTypeName(i);
                if (columnTypeName.matches(Code.MACHE_REGEX)){
                    String string = rs.getString(i);
                    if (null == string) { joiner.add("NULL");continue; }
                    joiner.add(new String(string.getBytes("cp850"),"utf-8"));
                }else {
                    try { joiner.add(rs.getString(i)); } catch (Exception e) { joiner.add(" "); }
                }
            }
            System.out.println(joiner.toString());
        }
        rs.close();
    }
    public static void insertOne(String sql, Connection connection) throws Exception {
        PreparedStatement pstmt = connection.prepareStatement(sql);
        pstmt.setString(1, new String("這個是一段正體字文字，我要把它從cp850轉成utf-8".getBytes("utf-8"), "cp850")); // 使用setBytes方法设置二进制数据
        pstmt.executeUpdate();
    }
    public static void main(String[] args) {
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            Connection conn = DriverManager.getConnection("jdbc:jtds:sybase://139.198.105.8:45000/testdb", "", "");
            Statement statement = conn.createStatement();
            insertOne("INSERT INTO tester.poc_test (varchar_col) VALUES (?)", conn);
            select("select top 1 * from tester.poc_test order by id desc", statement);
            conn.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        try {
            byte[] b = "µêæÕ¥êÕÑ¢".getBytes("cp850");//编码
            System.out.println(new String(b, "utf-8"));;
        } catch (Exception e){

        }

        String regex = "(.*)(CHAR|char|TEXT|text)(.*)";
        System.out.println("TEXT: " + "TEXT".matches(regex));
        System.out.println("NCHAR: " + "NCHAR".matches(regex));
        System.out.println("UNICHAR: " + "UNICHAR".matches(regex));
        System.out.println("VARCHAR: " + "VARCHAR".matches(regex));
        System.out.println("NVARCHAR: " + "NVARCHAR".matches(regex));
        System.out.println("UNIVARCHAR: " + "UNIVARCHAR".matches(regex));
        System.out.println("UNITEXT: " + "UNITEXT".matches(regex));

        System.out.println("DOUBLE: " + "DOUBLE".matches(regex));
        System.out.println("varchar: " + "varchar".matches(regex));
        System.out.println("VARCHAR: " + "VARCHAR".matches(regex));
        System.out.println("double: " + "double".matches(regex));
        System.out.println("text: " + "text".matches(regex));
        System.out.println("INT: " + "INT".matches(regex));
    }
}
