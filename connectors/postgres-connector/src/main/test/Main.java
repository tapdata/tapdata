//import io.tapdata.common.DataSourcePool;
//import io.tapdata.connector.postgres.PostgresJdbcContext;
//import io.tapdata.connector.postgres.config.PostgresConfig;
//import io.tapdata.entity.utils.DataMap;
//
//import java.sql.Connection;
//import java.sql.ResultSet;
//
//public class Main {
//    public static void main(String[] args) throws Throwable {
//        DataMap map = new DataMap();
//        map.put("host", "192.168.1.189");
//        map.put("port", 5432);
//        map.put("database", "COOLGJ");
//        map.put("schema", "public");
//        map.put("extParams", "");
//        map.put("user", "postgres");
//        map.put("password", "gj0628");
//        PostgresConfig postgresConfig = (PostgresConfig) new PostgresConfig().load(map);
////        postgresConfig.setHost("192.168.1.189");
////        postgresConfig.setPort(5432);
////        postgresConfig.setDatabase("COOLGJ");
////        postgresConfig.setSchema("public");
////        postgresConfig.setExtParams("");
////        postgresConfig.setUser("postgres");
////        postgresConfig.setPassword("gj0628");
//        PostgresJdbcContext postgresJdbcContext = (PostgresJdbcContext) DataSourcePool.getJdbcContext(postgresConfig, PostgresJdbcContext.class);
//        Connection connection = postgresJdbcContext.getConnection();
//        ResultSet rs = connection.createStatement().executeQuery("SELECT col.*, d.description,\n" +
//                "       (SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS \"dataType\"\n" +
//                "        FROM pg_catalog.pg_attribute a\n" +
//                "        WHERE a.attnum > 0\n" +
//                "          AND a.attname = col.column_name\n" +
//                "          AND NOT a.attisdropped\n" +
//                "          AND a.attrelid =\n" +
//                "              (SELECT cl.oid\n" +
//                "               FROM pg_catalog.pg_class cl\n" +
//                "                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = cl.relnamespace\n" +
//                "               WHERE cl.relname = col.table_name))\n" +
//                "FROM information_schema.columns col\n" +
//                "         JOIN pg_class c ON c.relname = col.table_name\n" +
//                "         LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = col.ordinal_position\n" +
//                "WHERE col.table_catalog='COOLGJ' AND col.table_schema='public' AND col.table_name='TestD' \n" +
//                "ORDER BY col.table_name,col.ordinal_position");
//        while (rs.next()) {
//            System.out.println(rs.getString("column_default"));
//        }
//        postgresJdbcContext.finish();
//
////        postgresJdbcContext.query("select * from \"Student\"", rs -> {
////            rs.last();
////            System.out.println(rs.getRow());
////        });
//    }
//}
