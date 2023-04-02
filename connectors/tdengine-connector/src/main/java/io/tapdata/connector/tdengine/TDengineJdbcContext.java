package io.tapdata.connector.tdengine;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.tdengine.config.TDengineConfig;
import io.tapdata.connector.tdengine.kit.TDengineDbKit;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * @author IssaacWang
 * @create 2022-10-10
 **/
public class TDengineJdbcContext extends JdbcContext {

    private static final String TAG = TDengineJdbcContext.class.getSimpleName();

    public static final String DATABASE_TIMEZON_SQL = "select TIMEZONE()";

    public TDengineJdbcContext(TDengineConfig config) {
        super(config);
    }

    @Override
    public String queryVersion() {
        return null;
    }

    public String timezone() throws Exception {

        String timezone = null;
        TapLogger.debug(TAG, "Get timezone sql: " + DATABASE_TIMEZON_SQL);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(DATABASE_TIMEZON_SQL)
        ) {
            while (resultSet.next()) {
                timezone = resultSet.getString(1);
            }
        }
        return timezone;
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getDatabase());
        List<DataMap> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format("select table_name, table_comment, type from information_schema.ins_tables where db_name = '%s' %s;", getConfig().getDatabase(), tableSql),
                    resultSet -> tableList.addAll(TDengineDbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }

    @Override
    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getDatabase());
        List<String> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format("select table_name, table_comment, type from information_schema.ins_tables where db_name = '%s' %s;", getConfig().getDatabase(), tableSql),
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
        TapLogger.debug(TAG, "Query columns of some tables, schema: " + getConfig().getDatabase());
        List<DataMap> columnList = TapSimplify.list();
        try {
            for (String tableName : tableNames) {
                String sql = String.format("DESCRIBE %s.%s", getConfig().getDatabase(), tableName);
                query(sql, resultSet -> {
                    List<DataMap> list = TDengineDbKit.getDataFromResultSet(resultSet);
                    list.forEach(dataMap -> dataMap.put("table_name", tableName));
                    String type = list.get(0).getString("type");
                    if (StringUtils.equalsIgnoreCase("TIMESTAMP", type)) {
                        list.get(0).put("is_primary", Boolean.TRUE);
                    }
                    columnList.addAll(list);
                });
            }
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        return Collections.emptyList();
    }

    public boolean tableExists(String tableName) throws Throwable {
        AtomicBoolean exists = new AtomicBoolean();
        String sql = String.format("SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'", getConfig().getDatabase(), tableName);
        query(sql, rs -> exists.set(rs.next()));
        return exists.get();
    }

}
