/**
 * @Author dayun
 * @Date 7/14/22
 */
import io.tapdata.connector.doris.bean.DorisConfig;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;

@DisplayName("Bench Test")
public class BenchTest {
    private DorisConfig dorisConfig;
    private Connection conn;

    private void initConnection() throws Exception {
        if (conn == null) {
            String targetPath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\doris-connector\\src\\main\\resources\\target.json";
            dorisConfig = (DorisConfig) new DorisConfig().load(targetPath);
            String dbUrl = dorisConfig.getDatabaseUrl();
            conn = DriverManager.getConnection(dbUrl, dorisConfig.getUser(), dorisConfig.getPassword());
        }
    }

    @Test
    @Disabled("Disabled")
    @DisplayName("Basic write benchTest")
    void writeBenchTest() throws Exception {
        initConnection();
        Statement statement = conn.createStatement();
        statement.execute("truncate table createTableTest__doris_344a5591_68b1_4d4f_85e0_dc08086323cc");
        Instant startTime = Instant.now();
        String preparedSQL = "INSERT INTO createTableTest__doris_344a5591_68b1_4d4f_85e0_dc08086323cc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStatement = conn.prepareStatement(preparedSQL);
        for (int i = 0; i < 10000; i++) {
            preparedStatement.setObject(1,"id_" + i);
            preparedStatement.setObject(2,"123");
            preparedStatement.setObject(3,"{\"age\":0,\"description\":\"desp_0\",\"gender\":10,\"id\":\"uid_0\",\"name\":\"name_0\"}");
            preparedStatement.setObject(4,1234567890);
            preparedStatement.setObject(5,1);
            preparedStatement.setObject(6,123123);
            preparedStatement.setObject(7,1);
            preparedStatement.setObject(8,"2022-04-02 20:34:16");
            preparedStatement.setObject(9,"[\"1\",\"2\",\"3\"]");
            preparedStatement.setObject(10,"[1.1,2.2,3.3]");
            preparedStatement.setObject(11,"[{\"age\":1,\"description\":\"d\",\"gender\":1,\"id\":\"a\",\"name\":\"n\"},{\"age\":2,\"description\":\"b\",\"gender\":10,\"id\":\"b\",\"name\":\"a\"}]");
            preparedStatement.setObject(12,"{\"age\":11,\"description\":\"d1\",\"gender\":1,\"id\":\"a1\",\"name\":\"n1\"}");
            preparedStatement.setObject(13,123.0);
            preparedStatement.setObject(14,343.22);
            preparedStatement.setObject(15,"exUDAg==");
            preparedStatement.setObject(16,"2022-04-02 20:34:16");
            preparedStatement.setObject(17,"{\"a\":\"a\",\"b\":\"b\"}");
            preparedStatement.setObject(18,"{\"a\":1.0,\"b\":2.0}");
            preparedStatement.setObject(19,"{\"a\":\"{\"age\":11,{\"a\":\"{\"age\":11,\"description\":\"d1\",\"gender\":1,\"id\":\"a1\",\"name\":\"n1\"}\"}\"description\":\"d1\",\"gender\":1,\"id\":\"a1\",\"name\":\"n1\"}\"}");
            preparedStatement.setObject(20,"2022-04-02 20:34:16");
            preparedStatement.setObject(21,"2022-04-02 20:34:16");
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        System.out.println("Millis:" + Duration.between(startTime, Instant.now()).toMillis());
        System.out.println("QPS : "  + 10000.0 / Duration.between(startTime, Instant.now()).toMillis());
        preparedStatement.close();
        statement.execute("truncate table createTableTest__doris_344a5591_68b1_4d4f_85e0_dc08086323cc");
        statement.close();
    }

}

