import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hive.config.HiveConfig;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "192.168.1.189");
        map.put("port", 20000);
        map.put("database", "default");
        map.put("user", "hive");
        map.put("password", "hive");
        HiveConfig hiveConfig = (HiveConfig) new HiveConfig().load(map);
        HiveJdbcContext hiveJdbcContext = new HiveJdbcContext(hiveConfig);
        hiveJdbcContext.query("select * from PERSONS_TEST_002", resultSet -> {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        });
        hiveJdbcContext.close();
    }
}
