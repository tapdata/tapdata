package io.tapdata.sybase.util;

import java.sql.Connection;
import java.sql.Date;
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

    public static final String MACHE_REGEX = "(.*)(CHAR|char|TEXT|text|sysname|SYSNAME)(.*)";

    public static void select(String sql, Statement statement) throws Exception {
        ResultSet rs = statement.executeQuery(sql);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            StringJoiner joiner = new StringJoiner(" --- ");
            for (int i = 1; i < columnCount + 1; i++) {
                String columnTypeName = metaData.getColumnTypeName(i);
                if (columnTypeName.matches(Code.MACHE_REGEX)) {
                    String string = rs.getString(i);
                    if (null == string) {
                        joiner.add("NULL");
                        continue;
                    }
                    joiner.add(new String(string.getBytes("cp850"), "big5-hkscs"));
                } else {
                    try {
                        joiner.add(rs.getString(i));
                    } catch (Exception e) {
                        joiner.add(" ");
                    }
                }
            }
            System.out.println(joiner.toString());
        }
        rs.close();
    }


    public static final String INSERT_POC_TEST_SQL = "insert into tester.poc_test_no_txt (" +
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

    public static void insertOne(String sql, Connection connection) throws Exception {
        PreparedStatement pstmt = connection.prepareStatement(sql);
        pstmt.setString(1, new String("這個是一段正體字文字，我要把它從cp850轉成utf-8".getBytes("big5-hkscs"), "cp850")); // 使用setBytes方法设置二?制?据
        pstmt.executeUpdate();
    }

    public static void insert(String sql, Connection connection) throws Exception {
        PreparedStatement p = connection.prepareStatement(sql);
        int index = 1;
        p.setString(index++, new String("F".getBytes("utf-8"), "big5"));
        p.setDate(index++, new Date(System.currentTimeMillis()));
        p.setDouble(index++, 2.36);
        p.setDouble(index++, 2.36);
        p.setInt(index++, 8);
        p.setDouble(index++, 3.33);
        p.setDouble(index++, 4.33);
        p.setString(index++, new String("B這個是一段正體字文字，我要把它從cp850轉成utf-8".getBytes("big5-hkscs"), "cp850")); // 使用setBytes方法设置二?制?据
        p.setDate(index++, new Date(System.currentTimeMillis()));
        p.setInt(index++, 3);
        p.setString(index++, new String("BFdsd".getBytes("utf-8"), "big5"));
//        p.setString(index++, new String(("" +
//                "，我要把它從cp850轉成utf-8").getBytes("big5-hkscs"), "cp850")); // 使用setBytes方法设置二?制?据
        p.setInt(index++, 3);
        p.setString(index++, new String("V這個是一段正體字文字，我要把它從cp850轉成utf-8".getBytes("big5-hkscs"), "cp850")); // 使用setBytes方法设置二?制?据
        p.executeUpdate();
    }

    public static void main(String[] args) {

//        Map<String, Object> before = new HashMap<>();
//        before.put("ccc_tail", "ccc");
//        before.put("ccc_head", "head");
//        before.put("big5", "fdsf");
//        before.put("phonetic_text", "fdsf");
//        before.put("unicode_int", 12.6);
//
//        List<String> uniqueCondition = new ArrayList<>();
//        uniqueCondition.add("ccc_tail");
//        uniqueCondition.add("ccc_head");
//        uniqueCondition.add("unicode_int");
//        before.keySet().removeIf(k -> !uniqueCondition.contains(k));
//        System.out.println(before.size());


//        String a = "root      5477     1  0 07:06 ?        00:00:00 /bin/sh -c export JAVA_TOOL_OPTIONS=\"-Duser.language=en\"; /tapdata/apps/sybase-poc/replicant-cli/bin/replicant real-time /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/src_sybasease.yaml /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/dst_localstorage.yaml --general /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/general.yaml --filter /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/filter_sybasease.yaml --extractor /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/ext_sybasease.yaml --id b5a9c529fd164b5 --replace --overwrite --verbose";
//        String b1 = "root      5538  5477  4 07:06 ?        00:00:09 java sh /tapdata/apps/sybase-poc/replicant-cli/bin/replicant real-time /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/src_sybasease.yaml /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/dst_localstorage.yaml --general /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/general.yaml --filter /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/filter_sybasease.yaml --extractor /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/ext_sybasease.yaml --id b5a9c529fd164b5 --replace --overwrite --verbose";
//        String c = "root      6013  4602  0 07:10 pts/16   00:00:00 grep java -Duser.timezone=UTC -Djava.system.class.loader=tech.replicant.util.ReplicantClassLoader -classpath /tapdata/apps/sybase-poc/replicant-cli/target/replicant-core.jar:/tapdata/apps/sybase-poc/replicant-cli/lib/ts-5089.jar:/tapdata/apps/sybase-poc/replicant-cli/lib/ts.jar:/tapdata/apps/sybase-poc/replicant-cli/lib/* tech.replicant.Main real-time /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/src_sybasease.yaml /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/dst_localstorage.yaml --general /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/general.yaml --filter /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/filter_sybasease.yaml --extractor /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/ext_sybasease.yaml --id b5a9c529fd164b5 --replace --overwrite --verbose";
//
//        String[] split = a.split("( )+");
//        String[] split1 = b1.split("( )+");
//        String[] split2 = c.split("( )+");
//        System.out.println();

        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            Connection conn = DriverManager.getConnection("jdbc:jtds:sybase://139.198.105.8:45000/testdb", "", "");
            Statement statement = conn.createStatement();
            //insertOne("INSERT INTO tester.poc_test (varchar_col) VALUES (?)", conn);
            for (int i = 0; i < 100000; i++) {
                insert(INSERT_POC_TEST_SQL, conn);
            }

            //select("select top 1 * from tester.poc_test order by id desc", statement);
            conn.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
//        try {
//            byte[] b = "?ê??¥ê??¢".getBytes("cp850");//??
//            System.out.println(new String(b, "utf-8"));;
//        } catch (Exception e){
//
//        }
//
//        String regex = "(.*)(CHAR|char|TEXT|text)(.*)";
//        System.out.println("TEXT: " + "TEXT".matches(regex));
//        System.out.println("NCHAR: " + "NCHAR".matches(regex));
//        System.out.println("UNICHAR: " + "UNICHAR".matches(regex));
//        System.out.println("VARCHAR: " + "VARCHAR".matches(regex));
//        System.out.println("NVARCHAR: " + "NVARCHAR".matches(regex));
//        System.out.println("UNIVARCHAR: " + "UNIVARCHAR".matches(regex));
//        System.out.println("UNITEXT: " + "UNITEXT".matches(regex));
//
//        System.out.println("DOUBLE: " + "DOUBLE".matches(regex));
//        System.out.println("varchar: " + "varchar".matches(regex));
//        System.out.println("VARCHAR: " + "VARCHAR".matches(regex));
//        System.out.println("double: " + "double".matches(regex));
//        System.out.println("text: " + "text".matches(regex));
//        System.out.println("INT: " + "INT".matches(regex));
    }
}
