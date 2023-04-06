package io.tapdata.connector.hive;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.list;

public class HiveJdbcContext extends JdbcContext {

    public HiveJdbcContext(HiveConfig config) {
        super(config);
    }

    public Connection getConnection() throws SQLException {
        Connection connection = super.getConnection();
        connection.setAutoCommit(true);
        return connection;
    }

    public List<DataMap> queryAllTables(List<String> tableNames) throws SQLException {
        List<DataMap> tableList = list();
        query(HIVE_ALL_TABLE, resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        if (EmptyKit.isNotEmpty(tableNames)) {
            return tableList.stream().filter(t -> tableNames.contains(t.getString("tab_name"))).collect(Collectors.toList());
        }
        return tableList;
    }

    private final static String HIVE_ALL_TABLE = "show tables";
}
