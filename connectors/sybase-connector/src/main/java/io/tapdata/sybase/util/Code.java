package io.tapdata.sybase.util;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
        p.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
        p.setDouble(index++, 2.36);
        p.setDouble(index++, 2.36);
        p.setInt(index++, 8);
        p.setDouble(index++, 3.33);
        p.setDouble(index++, 4.33);
        p.setString(index++, new String("B這個是一段正體字文字，我要把它從cp850轉成utf-8".getBytes("big5-hkscs"), "cp850")); // 使用setBytes方法设置二?制?据
        p.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
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

//        try {
//            String f = "2003-07-20 05:41:39.436";
//            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//            java.util.Date parse = format.parse(f);
//            long time = parse.getTime();
//            System.out.println(time);
//        } catch (Exception e){
//
//        }


//        String txt = "[{\"phase\":56332334906815740,\"payWayDesc\":\"支付平台\",\"expressPay\":false,\"payTime\":\"20230509215039000+0800\",\"payStatusDesc\":\"已付款\",\"payWay\":\"13\",\"cardPay\":false,\"payStatus\":\"6\",\"phasAmount\":1860}],\"nativeLogistics\":{\"area\":\"宝安区\",\"zip\":\"518101\",\"address\":\"广东省 深圳市 宝安区\",\"city\":\"深圳市\",\"contactPerson\":\"李生\",\"areaCode\":\"440306\",\"province\":\"广东省\"},\"baseInfo\":{\"allDeliveredTime\":\"20230510104404000+0800\",\"businessType\":\"cn\",\"buyerID\":\"b2b-23814857\",\"completeTime\":\"20230512160002000+0800\",\"createTime\":\"20230509214355000+0800\",\"id\":3339525816246816000,\"modifyTime\":\"20230601193805000+0800\",\"payTime\":\"20230509215039000+0800\",\"refund\":1860,\"sellerID\":\"b2b-42433677729db20\",\"shippingFee\":60,\"status\":\"cancel\",\"totalAmount\":1860,\"discount\":-19000,\"buyerContact\":{\"phone\":\"\",\"imInPlatform\":\"tshades\",\"name\":\"李文华\"},\"sellerContact\":{\"phone\":\"86-0663-17880500782\",\"imInPlatform\":\"戴格斯家用电器\",\"name\":\"林楷旭\",\"mobile\":\"17880500782\",\"companyName\":\"揭阳空港区京冈戴格斯家用电器厂\"},\"tradeType\":\"50060\",\"refundStatus\":\"refundsuccess\",\"refundPayment\":186000,\"idOfStr\":\"3339525816246815748\",\"alipayTradeId\":\"11210600023050961163010814857\",\"receiverInfo\":{\"toDivisionCode\":\"440306\",\"toArea\":\"广东省 深圳市 宝安区\",\"toFullName\":\"李生\",\"toPost\":\"518101\"},\"buyerLoginId\":\"tshades\",\"sellerLoginId\":\"戴格斯家用电器\",\"buyerUserId\":23814857,\"sellerUserId\":4243367772,\"buyerAlipayId\":\"2088002022016601\",\"sellerAlipayId\":\"2088332373643152\",\"sumProductPayment\":1995,\"stepPayAll\":false,\"overSeaOrder\":false},\"productItems\":[{\"cargoNumber\":\"001\",\"itemAmount\":1800,\"name\":\"新款家用二合一空气净化器 室内桌面除甲醛除烟小型台灯净化机\",\"price\":33.25,\"productID\":676651385016,\"productImgUrl\":[\"http://cbu01.alicdn.com/img/ibank/O1CN01euEhA31Bs2nYtrGOn_!!0-0-cib.80x80.jpg\",\"http://cbu01.alicdn.com/img/ibank/O1CN01euEhA31Bs2nYtrGOn_!!0-0-cib.jpg\"],\"productSnapshotUrl\":\"https://trade.1688.com/order/offer_snapshot.htm?order_entry_id=3339525816246815748\",\"quantity\":60,\"refund\":1800,\"s";
//        System.out.println(txt.length());

        try {
//            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
//            java.util.Date parse = format.parse("2023-07-28 10:13:26.185746");
//            System.out.println(parse.getTime());
//            System.out.println(parse.toInstant().getNano());
//            System.out.println(new Date(parse.getTime()).getTime());
//            System.out.println(new Date(parse.getTime()).toInstant().getNano());

//            Class.forName("net.sourceforge.jtds.jdbc.Driver");
//            Connection conn = DriverManager.getConnection("jdbc:jtds:sybase://139.198.105.8:45000/testdb", "tester", "guest1234");
//            Statement statement = conn.createStatement();
//            //insertOne("INSERT INTO tester.poc_test (varchar_col) VALUES (?)", conn);
//            conn.setAutoCommit(false);
//            for (int i = 0; i < 10; i++) {
//                insert(INSERT_POC_TEST_SQL, conn);
//            }
//            conn.rollback();
//
//            //select("select top 2 * from tester.poc_test_no_id order by datetime_col asc", statement);
//            conn.close();
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
