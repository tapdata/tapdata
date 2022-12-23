package io.tapdata.connector.selectdb;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.JdbcContext;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.selectdb.config.SelectDbConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Author:Skeet
 * Date: 2022/12/8 16:24
 **/
public class SelectDbJdbcContext extends JdbcContext {
    private static final String TAG = SelectDbJdbcContext.class.getSimpleName();

    public SelectDbJdbcContext(SelectDbConfig config, HikariDataSource hikariDataSource) {
        super(config, hikariDataSource);
    }

    public void query(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            statement.setFetchSize(1000);
            if (null != resultSet) {
                resultSetConsumer.accept(resultSet);
            }
        } catch (SQLException e) {
            throw new Exception("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    @Override
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            queryWithNext("show variables where variable_name = 'version'", resultSet -> version.set(resultSet.getString(1)));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return version.get();
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        List<DataMap> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(SDB_ALL_TABLE, getConfig().getDatabase(), getConfig().getSchema(), tableSql),
                    resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }

    @Override
    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        List<String> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(SDB_ALL_TABLE, getConfig().getDatabase(), getConfig().getSchema(), tableSql),
                    resultSet -> {
                        while (resultSet.next()) {
                            String tableName = resultSet.getString("table_name");
                            if (StringUtils.isNotBlank(tableName)) {
                                tableList.add(tableName);
                            }
                            if (tableList.size() >= batchSize) {
                                consumer.accept(tableList);
                                tableList.clear();
                            }
                        }
                    });
            if (!tableList.isEmpty()) {
                consumer.accept(tableList);
                tableList.clear();
            }
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {
        return null;
    }

    public Map<String,List<DataMap>> queryAllColumnsGroupByTableName(List<String> tableNames) {
        StringJoiner joiner = new StringJoiner(",");
        tableNames.stream().filter(Objects::nonNull).forEach(tab->joiner.add("'"+tab+"'"));
        TapLogger.debug(TAG, "Query columns of some tables, schema: " + getConfig().getSchema());
        List<DataMap> columnList = TapSimplify.list();
        //String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(SDB_ALL_COLUMN, getConfig().getDatabase(), joiner.toString()),
                    resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(field -> String.valueOf(field.get("TABLE_NAME"))));
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        TapLogger.debug(TAG, "Query indexes of some tables, schema: " + getConfig().getSchema());
        List<DataMap> indexList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(SDB_ALL_INDEX, getConfig().getDatabase(), getConfig().getSchema(), tableSql),
                    resultSet -> indexList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }


    private static final String TABLE_NAME_IN = " AND TABLE_NAME IN(%s)";

    private static final String SELECT_TABLES = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE='BASE TABLE'";

    private final static String SDB_ONE_TABLE = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s'";

    private final static String SDB_ALL_TABLE = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE='BASE TABLE'";

    private final static String SDB_ALL_COLUMN = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME in (%s)";

    private final static String SDB_ALL_INDEX = "select i.TABLE_NAME,\n" +
            "i.INDEX_NAME,\n" +
            "i.INDEX_TYPE,\n" +
            "i.COLLATION,\n" +
            "i.NON_UNIQUE,\n" +
            "i.COLUMN_NAME,\n" +
            "i.SEQ_IN_INDEX,\n" +
            "k.CONSTRAINT_NAME\n" +
            "from INFORMATION_SCHEMA.STATISTICS i\n" +
            "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE k\n" +
            "on k.TABLE_NAME = i.TABLE_NAME and i.COLUMN_NAME = k.COLUMN_NAME\n" +
            "where k.TABLE_SCHEMA = '%s'\n" +
            "and i.TABLE_SCHEMA = '%s'\n" +
            "and i.TABLE_NAME %s\n" +
            "and i.INDEX_NAME <> 'PRIMARY'";

}