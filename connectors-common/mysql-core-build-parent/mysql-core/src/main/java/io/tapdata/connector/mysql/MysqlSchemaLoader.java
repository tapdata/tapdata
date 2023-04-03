package io.tapdata.connector.mysql;

import com.google.common.collect.Lists;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.kit.DbKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.index;
import static io.tapdata.entity.simplify.TapSimplify.indexField;

/**
 * @author samuel
 * @Description
 * @create 2022-04-26 20:09
 **/
public class MysqlSchemaLoader {
    private static final String TAG = MysqlSchemaLoader.class.getSimpleName();
    private static final String SELECT_TABLES = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE='BASE TABLE'";
    private static final String TABLE_NAME_IN = " AND TABLE_NAME IN(%s)";
    private static final String SELECT_COLUMNS = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME %s";
    private final static String SELECT_ALL_INDEX_SQL = "select TABLE_NAME,INDEX_NAME,INDEX_TYPE,COLLATION,NON_UNIQUE,COLUMN_NAME,SEQ_IN_INDEX\n" +
            "from INFORMATION_SCHEMA.STATISTICS\n" +
            "where TABLE_SCHEMA = '%s'\n" +
            "and TABLE_NAME %s order by INDEX_NAME,SEQ_IN_INDEX";


    private TapConnectionContext tapConnectionContext;
    private MysqlJdbcContext mysqlJdbcContext;

    public MysqlSchemaLoader(MysqlJdbcContext mysqlJdbcContext) {
        this.mysqlJdbcContext = mysqlJdbcContext;
        this.tapConnectionContext = mysqlJdbcContext.getTapConnectionContext();
    }

    public void discoverSchema(List<String> filterTable, Consumer<List<TapTable>> consumer, int tableSize) throws Throwable {
        if (null == consumer) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }

        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        List<DataMap> allTables =queryAllTables(database, filterTable);
        if (CollectionUtils.isEmpty(allTables)) {
            consumer.accept(null);
            return;
        }

        TableFieldTypesGenerator instance = InstanceFactory.instance(TableFieldTypesGenerator.class);
        DefaultExpressionMatchingMap dataTypesMap = tapConnectionContext.getSpecification().getDataTypesMap();

        try {
            List<List<DataMap>> tableLists = Lists.partition(allTables, tableSize);
            tableLists.forEach(tableList -> {
                List<String> subTableNames = tableList.stream().map(v -> v.getString("TABLE_NAME")).collect(Collectors.toList());
                String tableNames = StringUtils.join(subTableNames, "','");
                List<DataMap> columnList = queryAllColumns(database, tableNames);
                List<DataMap> indexList = queryAllIndexes(database, tableNames);

                Map<String, List<DataMap>> columnMap = columnList.stream().collect(Collectors.groupingBy(t -> t.getString("TABLE_NAME")));
                Map<String, List<DataMap>> indexMap = indexList.stream().collect(Collectors.groupingBy(t -> t.getString("TABLE_NAME")));

                List<TapTable> tempList = new ArrayList<>();
                tableList.forEach(subTable -> {
                    String tableName = subTable.getString("TABLE_NAME");
                    TapTable tapTable = TapSimplify.table(tableName);
                    if (columnMap.containsKey(tableName)) {
                        discoverFields(columnMap.get(tableName), tapTable, instance, dataTypesMap);
                    }
                    if (indexMap.containsKey(tableName)) {
                        tapTable.setIndexList(discoverIndexes(indexMap.get(tableName), tableName));
                    }
                    tapTable.setComment(subTable.getString("TABLE_COMMENT"));
                    tempList.add(tapTable);
                });

                if (CollectionUtils.isNotEmpty(tempList)) {
                    consumer.accept(tempList);
                    tempList.clear();
                }
            });

        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    private void discoverFields(List<DataMap> columnList, TapTable tapTable, TableFieldTypesGenerator tableFieldTypesGenerator,
                                DefaultExpressionMatchingMap dataTypesMap) {
        AtomicInteger primaryPos = new AtomicInteger(1);

        if (CollectionUtils.isEmpty(columnList)) {
            return;
        }
        columnList.forEach(dataMap -> {
            String columnName = dataMap.getString("COLUMN_NAME");
            String columnType = dataMap.getString("COLUMN_TYPE");
            TapField field = TapSimplify.field(columnName, columnType);
            tableFieldTypesGenerator.autoFill(field, dataTypesMap);

            int ordinalPosition = Integer.parseInt(dataMap.getString("ORDINAL_POSITION"));
            field.pos(ordinalPosition);

            String isNullable = dataMap.getString("IS_NULLABLE");
            field.nullable(isNullable.equals("YES"));

            Object columnKey = dataMap.getObject("COLUMN_KEY");
            if (columnKey instanceof String && columnKey.equals("PRI")) {
                field.primaryKeyPos(primaryPos.getAndIncrement());
            }

//			String columnDefault = dataMap.getString("COLUMN_DEFAULT");
//			field.defaultValue(columnDefault);
            tapTable.add(field);
        });
    }

    private List<DataMap> queryAllColumns(String database, String tableNames) {
        TapLogger.debug(TAG, "Query all columns, database: {}, tableNames:{}", database, tableNames);

        String inTableName = new StringJoiner(tableNames).add("IN ('").add("')").toString();
        String sql = String.format(SELECT_COLUMNS, database, inTableName);
        List<DataMap> columnList = TapSimplify.list();
        try {
            mysqlJdbcContext.query(sql, resultSet -> {
                List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                while (resultSet.next()) {
                    columnList.add(DbKit.getRowFromResultSet(resultSet, columnNames));
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    private List<DataMap> queryAllIndexes(String database, String tableNames) {
        TapLogger.debug(TAG, "Query all indexes, database: {}, tableNames:{}", database, tableNames);
        List<DataMap> indexList = TapSimplify.list();

        String inTableName = new StringJoiner(tableNames).add("IN ('").add("')").toString();
        String sql = String.format(SELECT_ALL_INDEX_SQL, database, inTableName);
        try {
            mysqlJdbcContext.query(sql, resultSet -> {
                List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                while (resultSet.next()) {
                    indexList.add(DbKit.getRowFromResultSet(resultSet, columnNames));
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }

    public List<TapIndex> discoverIndexes(String database, String tableName) {
        return discoverIndexes(queryAllIndexes(database, tableName), tableName);
    }

    public List<TapIndex> discoverIndexes(List<DataMap> indexList, String tableName) {
        List<TapIndex> tapIndexList = TapSimplify.list();
        Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> tableName.equals(idx.getString("TABLE_NAME")))
                .collect(Collectors.groupingBy(idx -> idx.getString("INDEX_NAME"), LinkedHashMap::new, Collectors.toList()));
        indexMap.forEach((key, value) -> tapIndexList.add(makeTapIndex(key, value)));
        return tapIndexList;
    }

    private TapIndex makeTapIndex(String key, List<DataMap> value) {
        TapIndex index = index(key);
        value.forEach(v -> index.indexField(indexField(v.getString("COLUMN_NAME")).fieldAsc("A".equals(v.getString("COLLATION")))));
        index.setUnique(value.stream().anyMatch(v ->  "0".equals(v.getString("NON_UNIQUE"))));
        index.setPrimary(value.stream().anyMatch(v -> "PRIMARY".equals(v.getString("INDEX_NAME"))));
        return index;
    }

    private  List<DataMap>  queryAllTables(String database, List<String> filterTable) {
        String sql = String.format(SELECT_TABLES, database);
        if (CollectionUtils.isNotEmpty(filterTable)) {
            filterTable = filterTable.stream().map(t -> "'" + t + "'").collect(Collectors.toList());
            String tableNameIn = String.join(",", filterTable);
            sql += String.format(TABLE_NAME_IN, tableNameIn);
        }

        List<DataMap> tableList = TapSimplify.list();
        try {
            mysqlJdbcContext.query(sql, resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }

    public void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        String sql = String.format(SELECT_TABLES, database);
        List<String> list = new ArrayList<>();
        try {
            mysqlJdbcContext.query(sql, rs -> {
                while (rs.next()) {
                    list.add(rs.getString("TABLE_NAME"));
                    if (list.size() >= batchSize) {
                        listConsumer.accept(list);
                        list.clear();
                    }
                }
            });
        } catch (Throwable e) {
            throw new RuntimeException("Get table names failed, sql: " + sql + ", error: " + e.getMessage(), e);
        }
        if (list.size() > 0) {
            listConsumer.accept(list);
            list.clear();
        }
    }
}
