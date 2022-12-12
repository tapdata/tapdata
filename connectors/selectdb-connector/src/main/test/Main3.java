//import io.tapdata.common.DataSourcePool;
//import io.tapdata.connector.postgres.PostgresJdbcContext;
//import io.tapdata.connector.postgres.config.PostgresConfig;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//
//public class Main3 {
//    public static void main(String[] args) throws Throwable {
//        PostgresConfig postgresConfig = new PostgresConfig();
//        postgresConfig.setHost("192.168.1.189");
//        postgresConfig.setPort(5432);
//        postgresConfig.setDatabase("COOLGJ");
//        postgresConfig.setSchema("public");
//        postgresConfig.setExtParams("");
//        postgresConfig.setUser("postgres");
//        postgresConfig.setPassword("gj0628");
//        PostgresJdbcContext postgresJdbcContext = (PostgresJdbcContext) DataSourcePool.getJdbcContext(postgresConfig, PostgresJdbcContext.class);
//        Connection connection = postgresJdbcContext.getConnection();
////        PreparedStatement preparedStatement = connection.prepareStatement(
////                "DELETE FROM \"PgTest1234\" WHERE \"ddd\"=? or ((COALESCE(\"ddd\", null) is null) and (COALESCE(?, null) is null))");
//        PreparedStatement preparedStatement = connection.prepareStatement(
//                "INSERT INTO \"PgTest1234\" VALUES ('hhhh',?,'2020-01-02',?)");
//        preparedStatement.setObject(1, "55");
//        preparedStatement.setObject(2, "56767.8");
//        preparedStatement.addBatch();
//        preparedStatement.executeBatch();
//        connection.commit();
//        postgresJdbcContext.finish();
//
//
////        postgresJdbcContext.query("select * from \"Student\"", rs -> {
////            rs.last();
////            System.out.println(rs.getRow());
////        });
//    }
//}
