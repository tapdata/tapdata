package io.tapdata.connector.yashandb;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.JdbcContext;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * Author:Skeet
 * Date: 2023/5/22
 **/
public class YashandbJdbcContext extends JdbcContext {
    private static final String TAG = YashandbJdbcContext.class.getSimpleName();

    public YashandbJdbcContext(CommonDbConfig config) {
        super(config);
    }

    public String queryAllTables(String tableNames) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        StringBuilder stringBuilder = new StringBuilder();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? tableNames : "";
        String one = "";
        try {
            query(String.format(YSDB_ALL_TABLE, getConfig().getDatabase(), tableNames),
                    resultSet -> {
                        while (resultSet.next()) {
                            // 获取每一行的结果并添加到StringBuilder中
                            String rowResult = resultSet.getString(1); // 假设查询的结果为单列
                            stringBuilder.append(rowResult).append("\n");
                        }
                    });
            one = stringBuilder.toString();
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return one;
    }

    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) throws SQLException {
        List<String> temp = list();
        query(queryAllTablesSql(getConfig().getSchema(), tableNames),
                resultSet -> {
                    while (resultSet.next()) {
                        String tableName = resultSet.getString("TABLE_NAME");
                        if (EmptyKit.isNotBlank(tableName)) {
                            temp.add(tableName);
                        }
                        if (temp.size() >= batchSize) {
                            consumer.accept(temp);
                            temp.clear();
                        }
                    }
                });
        if (EmptyKit.isNotEmpty(temp)) {
            consumer.accept(temp);
            temp.clear();
        }
    }

    public Map<String, List<DataMap>> queryAllColumnsByTableName(List<String> tableNames) {
        StringJoiner joiner = new StringJoiner(",");
        tableNames.stream().filter(Objects::nonNull).forEach(tab -> joiner.add("'" + tab + "'"));
        TapLogger.debug(TAG, "Query columns of some tables, schema: " + getConfig().getSchema());
        List<DataMap> columnList = TapSimplify.list();
        //String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(YSDB_ALL_COLUMN, getConfig().getSchema(), joiner.toString()),
                    resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(field -> String.valueOf(field.get("TABLE_NAME"))));
    }
//    @Override
//    public List<DataMap> queryAllTables(List<String> tableNames) {
//        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
//        List<DataMap> tableList = TapSimplify.list();
//        String tableSql = EmptyKit.isNotEmpty(tableNames) ? StringKit.joinString(tableNames, "'", ","): "";
//        try {
//            query(String.format(YSDB_ALL_TABLE, getConfig().getDatabase(),tableSql),
//                    resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
//        } catch (Throwable e) {
//            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
//        }
//        return tableList;
//    }

//    @Override
//    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {
//        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
//        List<String> tableList = TapSimplify.list();
//        String tableSql = EmptyKit.isNotEmpty(tableNames) ? StringKit.joinString(tableNames, "'", ","): "";
//        try {
//            query(String.format(YSDB_ALL_TABLE, getConfig().getDatabase(), tableSql),
//                    resultSet -> {
//                        while (resultSet.next()) {
//                            String tableName = resultSet.getString("table_name");
//                            if (StringUtils.isNotBlank(tableName)) {
//                                tableList.add(tableName);
//                            }
//                            if (tableList.size() >= batchSize) {
//                                consumer.accept(tableList);
//                                tableList.clear();
//                            }
//                        }
//                    });
//            if (!tableList.isEmpty()) {
//                consumer.accept(tableList);
//                tableList.clear();
//            }
//        } catch (Throwable e) {
//            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
//        }
//    }


    @Override
    protected String queryAllTablesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        return String.format(YSDB_ALL_TABLE, getConfig().getSchema(), tableSql);
    }

    private final static String YSDB_ALL_TABLE = "SELECT table_name FROM ALL_TABLES WHERE OWNER = '%s' %s";
    private final static String YSDB_ALL_COLUMN =
            "SELECT * " +
            "FROM all_tab_columns " +
            "WHERE owner = '%s' AND table_name IN (%s)\n" +
            "ORDER BY owner, table_name";
}
