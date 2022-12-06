import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.utils.DataMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws Throwable {
        DataMap map = new DataMap();
        map.put("host", "192.168.1.183");
        map.put("port", 15432);
        map.put("database", "COOLGJ");
        map.put("schema", "public");
        map.put("extParams", "");
        map.put("user", "postgres");
        map.put("password", "gj0628");
        PostgresConfig postgresConfig = (PostgresConfig) new PostgresConfig().load(map);
//        postgresConfig.setHost("192.168.1.189");
//        postgresConfig.setPort(5432);
//        postgresConfig.setDatabase("COOLGJ");
//        postgresConfig.setSchema("public");
//        postgresConfig.setExtParams("");
//        postgresConfig.setUser("postgres");
//        postgresConfig.setPassword("gj0628");
        PostgresJdbcContext postgresJdbcContext = (PostgresJdbcContext) DataSourcePool.getJdbcContext(postgresConfig, PostgresJdbcContext.class, "ss");
        Connection connection = postgresJdbcContext.getConnection();
        String updateSql = "WITH upsert AS (UPDATE \"public\".\"aabbccdd\" SET \"aa\"=?,\"bb\"=?,\"game\"=?,\"FUCK\"=? WHERE \"aa\"=? RETURNING *)" +
                " INSERT INTO \"public\".\"aabbccdd\" (\"aa\",\"bb\",\"game\",\"FUCK\") SELECT ?,?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)";
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        CountDownLatch countDownLatch = new CountDownLatch(16);
        for(int j = 0; j < 16; j++) {
            executorService.submit(() -> {
                PreparedStatement ps = null;
                try {
                    ps = connection.prepareStatement(updateSql);
                    for (int i = 0; i < 1000; i++) {
                        ps.setObject(1, 90);
                        ps.setObject(2, "GJJ");
                        ps.setObject(3, "GJSJFADSFSDFADSFASDFSADFASDFSFASFASDFASDFASDFADSFADFASDFASDF");
                        ps.setObject(4, 718371);
                        ps.setObject(5, 90);
                        ps.setObject(6, 90);
                        ps.setObject(7, "GJJ");
                        ps.setObject(8, "GJSJFADSFSDFADSFASDFSADFASDFSFASFASDFASDFASDFADSFADFASDFASDF");
                        ps.setObject(9, 718371);
                        ps.addBatch();
                        ps.clearParameters();
                    }
                    ps.executeBatch();
                    ps.close();
                    connection.commit();
                    countDownLatch.countDown();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        countDownLatch.await();
        connection.close();
        postgresJdbcContext.finish("ss");
        executorService.shutdown();
//        postgresJdbcContext.query("select * from \"Student\"", rs -> {
//            rs.last();
//            System.out.println(rs.getRow());
//        });
    }
}
