package io.tapdata.connector.tencent.db.core;

import io.tapdata.connector.mysql.MysqlSchemaLoader;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TDSqlDiscoverSchema extends MysqlSchemaLoader {
    public final static String TAG = TDSqlDiscoverSchema.class.getSimpleName();
    public final static String PARTITION_KEY_SINGLE = "IS_PARTITION_KEY";
    public final static String PARTITION_KEY_SINGLE_NOT = "NOT_PARTITION_KEY";

    public static final String PARTITION_SQL = "Select PARTITION_EXPRESSION, TABLE_NAME\n" +
            "from information_schema.PARTITIONS\n" +
            "where\n" +
            "      TABLE_SCHEMA = %s and\n" +
            "      TABLE_NAME in ('%s')\n" +
            "group by TABLE_NAME";
    public static final String PARTITION_SQL_0 = "Select PARTITION_EXPRESSION, TABLE_NAME\n" +
            "from information_schema.PARTITIONS\n" +
            "where\n" +
            "      TABLE_SCHEMA = '%s' and\n" +
            "      TABLE_NAME = '%s'\n" +
            "group by TABLE_NAME";
    //public static final String SHOW_CREATE_TABLE = "SELECT create_statement FROM tables where name in (%s)";

    public TDSqlDiscoverSchema(MysqlJdbcContext mysqlJdbcContext) {
        super(mysqlJdbcContext);
    }

    private List<String> fullPartitionKey(String partitionRegex){
        List<String> keys = new ArrayList<>();
        if (null == partitionRegex || partitionRegex.trim().equals("")) return keys;
        int start = 21;
        int end = 0;
        while ((end = partitionRegex.indexOf(",", start)) > 0){
            String key = partitionRegex.substring(start, end).replaceAll("`","");
            keys.add(key);
            partitionRegex = partitionRegex.substring(end);
            start = 1;
        }
        return keys;
    }

    protected void discoverFields(List<DataMap> columnList, TapTable tapTable, TableFieldTypesGenerator tableFieldTypesGenerator,
                                  DefaultExpressionMatchingMap dataTypesMap) {
        AtomicInteger primaryPos = new AtomicInteger(1);

        if (CollectionUtils.isEmpty(columnList)) {
            return;
        }
        columnList.forEach(dataMap -> {
            String columnName = dataMap.getString("COLUMN_NAME");
            String columnType = dataMap.getString("COLUMN_TYPE");
            Boolean partitionKey = (Boolean)dataMap.get("IS_PARTITION_EXPRESSION");
            TapField field = TapSimplify.field(columnName, columnType);
            field.setPartitionKey(partitionKey);
            field.setComment(Optional.ofNullable(partitionKey).orElse(false) ? PARTITION_KEY_SINGLE :PARTITION_KEY_SINGLE_NOT);

            tableFieldTypesGenerator.autoFill(field, dataTypesMap);

            int ordinalPosition = Integer.parseInt(dataMap.getString("ORDINAL_POSITION"));
            field.pos(ordinalPosition);

            String isNullable = dataMap.getString("IS_NULLABLE");
            field.nullable(isNullable.equals("YES"));

            Object columnKey = dataMap.getObject("COLUMN_KEY");
            if (columnKey instanceof String && columnKey.equals("PRI")) {
                field.primaryKeyPos(primaryPos.getAndIncrement());
            }
            //String columnDefault = dataMap.getString("COLUMN_DEFAULT");
            //field.defaultValue(columnDefault);
            tapTable.add(field);
        });
    }

    public List<String> getAllPartitionKey(String database, String tableName){
        List<String> keys = new ArrayList<>();
        String sql = String.format(PARTITION_SQL_0, database, tableName);
        try {
            mysqlJdbcContext.query(sql, resultSet -> {
                List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                while (resultSet.next()) {
                    DataMap map = DbKit.getRowFromResultSet(resultSet, columnNames);
                    Object expression = map.get("PARTITION_EXPRESSION");
                    if (null != expression && tableName.equals(map.get("TABLE_NAME"))){
                        List<String> list = fullPartitionKey((String) expression);
                        if (!list.isEmpty()) {
                            keys.addAll(list);
                        }
                    }
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns partition failed, error: " + e.getMessage(), e);
        }
        return keys;
    }

    protected List<DataMap> queryAllColumns(String database, String tableNames) {
        TapLogger.debug(TAG, "Query all columns, database: {}, tableNames:{}", database, tableNames);

        Map<String,String> partitionMap = new HashMap<>();
        String sql = String.format(PARTITION_SQL, "'" + database + "'", tableNames);
        try {
            mysqlJdbcContext.query(sql, resultSet -> {
                List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                while (resultSet.next()) {
                    DataMap map = DbKit.getRowFromResultSet(resultSet, columnNames);
                    partitionMap.put((String) map.get("TABLE_NAME"), (String) map.get("PARTITION_EXPRESSION"));
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns partition failed, error: " + e.getMessage(), e);
        }

        String inTableName = new StringJoiner(tableNames).add("IN ('").add("')").toString();
        sql = String.format(SELECT_COLUMNS, database, inTableName);
        List<DataMap> columnList = TapSimplify.list();
        try {
            mysqlJdbcContext.query(sql, resultSet -> {
                List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                while (resultSet.next()) {
                    DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                    String tableName = (String)dataMap.get("TABLE_NAME");
                    String partitionName = partitionMap.get(tableName);
                    if (Objects.nonNull(partitionName)){
                        List<String> partitionNameArr = fullPartitionKey(partitionName);
                        String columnName = dataMap.getString("COLUMN_NAME");
                        dataMap.put("IS_PARTITION_EXPRESSION", partitionNameArr.contains(columnName));
                    }else {
                        dataMap.put("IS_PARTITION_EXPRESSION", false);
                    }
                    columnList.add(dataMap);
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;

    }

}
