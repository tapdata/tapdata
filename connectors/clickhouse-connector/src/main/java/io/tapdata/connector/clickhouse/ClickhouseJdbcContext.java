package io.tapdata.connector.clickhouse;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClickhouseJdbcContext extends JdbcContext {

    private final static String TAG = ClickhouseJdbcContext.class.getSimpleName();
    private String clickhouseVersion;

    public ClickhouseJdbcContext(ClickhouseConfig config) {
        super(config);
        exceptionCollector = new ClickhouseExceptionCollector();
        try {
            clickhouseVersion = queryVersion();
        } catch (SQLException ignored) {
        }
    }

    @Override
    protected String queryAllTablesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        if (Double.parseDouble(clickhouseVersion) > 21.8) {
            return String.format(CK_ALL_TABLE, schema, tableSql);
        } else {
            return String.format(CK_ALL_TABLE_20, schema, tableSql);
        }
    }

    @Override
    protected String queryAllColumnsSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        return String.format(CK_ALL_COLUMN, schema, tableSql);
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        return Collections.emptyList();
    }

    public DataMap getTableInfo(String tableName) {
        DataMap dataMap = DataMap.create();
        List<String> list = new ArrayList<>();
        list.add("NUM_ROWS");
        list.add("AVG_ROW_LEN");
        try {
            query(String.format(CK_TABLE_INFO, tableName, getConfig().getDatabase()), resultSet -> {
                while (resultSet.next()) {
                    dataMap.putAll(DbKit.getRowFromResultSet(resultSet, list));
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute getTableInfo failed, error: " + e.getMessage(), e);
        }
        return dataMap;
    }

    public String queryTimeZone() throws SQLException {
        String timeZone;
        TapLogger.debug(TAG, "Get timezone sql: " + DATABASE_TIMEZONE_SQL);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(DATABASE_TIMEZONE_SQL)
        ) {
            if (resultSet.next()) {
                timeZone = resultSet.getString(1);
                return timeZone;
            }
        }
        return null;
    }

    public static final String DATABASE_TIMEZONE_SQL = "SELECT timeZone()";
    private final static String CK_ALL_TABLE = "select name `tableName`, comment `tableComment` from system.tables where database='%s' %s";
    private final static String CK_ALL_TABLE_20 = "select name `tableName` from system.tables where database='%s' %s";
    private final static String CK_ALL_COLUMN =
            "select \n" +
                    "table `tableName`, \n" +
                    "name `columnName`, \n" +
                    "type `dataType`, \n" +
                    "default_expression `columnDefault`,\n" +
                    "comment `columnComment`,\n" +
                    "is_in_partition_key `isPartition`,\n" +
                    "is_in_sorting_key `isSorting`,\n" +
                    "is_in_primary_key `isPk`,\n" +
                    "is_in_sampling_key `isSampling`\n" +
                    "from system.columns \n" +
                    "where database='%s' %s\n" +
                    "order by table,position";
    private final static String CK_TABLE_INFO = "select * from system.tables where name ='%s' and database='%s' ";
}
