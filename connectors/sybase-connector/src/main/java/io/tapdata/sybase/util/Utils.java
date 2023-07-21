package io.tapdata.sybase.util;

import io.tapdata.entity.error.CoreException;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

/**
 * @author GavinXiao
 * @description Utils create by Gavin
 * @create 2023/7/10 19:13
 **/
public class Utils {
    public static Map<String, Object> obj2Map(Object obj) throws IllegalAccessException {
        Map<String, Object> map = new LinkedHashMap<>();
        if (obj == null) {
            return map;
        }
        Class<?> clazz = obj.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            String fieldName = field.getName();
            Object value = field.get(obj);
            if (value == null) {
                continue;
            }
            map.put(fieldName, value);
        }
        return map;
    }

    public static void main(String[] args) throws ClassNotFoundException {
        final String sql = "INSERT INTO tester.table_full_data_type(" +
                " field_id, field_bit, field_tinyint, field_smallint, field_unsigned_smallint, field_int, field_unsigned_int, field_bigint, field_unsigned_bigint," +
                " field_decimal, field_numeric, field_float, field_double_precision, field_real, field_smallmoney, field_money," +
                " field_date, field_time, field_bigtime, field_smalldatetime, field_datetime, field_bigdatetime, field_timestamp," +
                " field_char, field_nchar, field_unichar, field_varchar, field_nvarchar, field_univarchar, field_text, field_unitext," +
                " field_sysname, field_longsysname, field_binary, field_varbinary, field_image) VALUES ( %s )";
//        Class<?> aClass = Class.forName("net.sourceforge.jtds.jdbc.Driver");
//        Properties config = new Properties();


        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            Connection conn = DriverManager.getConnection("jdbc:jtds:sybase://139.198.105.8:45000/testdb", "", "");
            //ResultSet rs = conn.createStatement().executeQuery("select keys1 from sysindexes where id = 576002052");
            Statement statement = conn.createStatement();
            //insert(sql, statement);
            insertOne("INSERT INTO table_binary (field_varbinary) VALUES (?)", conn);
            select("select * from table_binary", statement);

            conn.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void insert(String sql, Statement statement) throws Exception {
        for (int i = 0; i < 10; i++) {
            statement.execute(String.format(sql,
                    (100000 + i) + ", " +
                            "1, " +
                            "2, " +
                            "3, " +
                            "4, " +
                            "5, " +
                            "6, " +
                            "7, " +
                            "8," +
                            "9, " +
                            "1, " +
                            "2.345, " +
                            "3.456, " +
                            "4.567, " +
                            "5.6780," +
                            " 6.7890," +
                            "'2018-05-23', " +
                            "'09:53:20', " +
                            "'09:53:45', " +
                            "'2018-05-23 10:01:00.0', " +
                            "'2018-05-23 10:01:01.0', " +
                            "'2018-05-23 10:01:01.0', " +
                            "0x0000000000002387," +
                            "'a', " +
                            "'b', " +
                            "'c', " +
                            "'d_111111', " +
                            "'e', " +
                            "'f', " +
                            "'g_111111', " +
                            "'h_111111'," +
                            "'i_111111', " +
                            "'j_111111', " +
                            "0x33, " +
                            "0x76617262696e6172795f76616c7565, " +
                            "NULL")
            );
        }
    }

    public static void insertOne(String sql, Connection connection) throws Exception {
        PreparedStatement pstmt = connection.prepareStatement(sql);
        pstmt.setBytes(1, "Hello, I'm Gavin!".getBytes(StandardCharsets.UTF_8)); // 使用setBytes方法设置二进制数据
        pstmt.executeUpdate();
    }

    public static void select(String sql, Statement statement) throws Exception {
        ResultSet rs = statement.executeQuery(sql);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            //System.out.println(rs.getString(1) + " "+ rs.getString(2) +" " + rs.getString(3));
            StringJoiner joiner = new StringJoiner(" --- ");
            for (int i = 1; i < columnCount + 1; i++) {
                try {
                    byte[] bytes = rs.getBytes(i);
                    joiner.add(rs.getString(i));
                } catch (Exception e) {
                    joiner.add(" ");
                }
            }
            System.out.println(joiner.toString());
        }
        rs.close();
    }

    public static Date dateFormat(String formatStr, String formatPatter) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(formatPatter);
        return format.parse(formatStr);
    }

//    public static void main(String[] args) throws ParseException {
//        System.out.println(dateFormat("2023-07-13 20:43:23.0", "yyyy-MM-dd HH:mm:ss.SSS").getTime());
//    }

    public static String run(String command) throws IOException {
        Scanner input = null;
        StringBuilder result = new StringBuilder();
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(command);
            try {
                process.waitFor(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            InputStream is = process.getInputStream();
            input = new Scanner(is);
            while (input.hasNextLine()) {
                result.append(input.nextLine()).append("\n");
            }
            result.insert(0, command + "\n"); //加上命令本身，打印出来
        } finally {
            if (input != null) {
                input.close();
            }
            if (process != null) {
                process.destroy();
            }
        }
        return result.toString();
    }

    public static String readFromInputStream(InputStream inputStream, Charset charsetName) throws IOException {
        Throwable var3 = null;
        String var4;
        try {
            var4 = IOUtils.toString(inputStream, Charsets.toCharset(charsetName));
        } catch (Throwable var13) {
            var3 = var13;
            throw new CoreException(var13.getMessage());
        } finally {
            if (inputStream != null) {
                if (var3 != null) {
                    try {
                        inputStream.close();
                    } catch (Throwable var12) {
                        var3.addSuppressed(var12);
                    }
                } else {
                    inputStream.close();
                }
            }
        }
        return var4;
    }

    public static String convertString(String fromValue, String fromCharset, String toCharset) throws UnsupportedEncodingException {
        if (null == fromValue) return null;
        if ("".equals(fromValue.trim())) return "";
        if (null == fromCharset || null == toCharset || "".equals(fromCharset.trim()) || "".equals(toCharset.trim())) return fromValue;
        byte[] b = fromValue.getBytes(fromCharset);//编码
        return new String(b, toCharset);
    }
}
