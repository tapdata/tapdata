package io.tapdata.oceanbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.kit.DbKit;
import io.tapdata.oceanbase.connector.OceanbaseJdbcContext;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author dayun
 * @date 2022/6/27 20:29
 */
public class OceanbaseSchemaLoader {
    private static final String TAG = OceanbaseSchemaLoader.class.getSimpleName();
    private static final String SELECT_TABLES = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE='BASE TABLE'";
    private static final String TABLE_NAME_IN = " AND TABLE_NAME IN(%s)";
    private static final String SELECT_COLUMNS = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME %s";
    private final static String SELECT_ALL_INDEX_SQL = "select *\n" +
            "from (select i.TABLE_NAME,\n" +
            "             i.INDEX_NAME,\n" +
            "             i.INDEX_TYPE,\n" +
            "             i.COLLATION,\n" +
            "             i.NON_UNIQUE,\n" +
            "             i.COLUMN_NAME,\n" +
            "             i.SEQ_IN_INDEX,\n" +
            "             (select k.CONSTRAINT_NAME\n" +
            "              from INFORMATION_SCHEMA.KEY_COLUMN_USAGE k\n" +
            "              where k.TABLE_SCHEMA = '%s'\n" +
            "                and k.TABLE_NAME = i.TABLE_NAME\n" +
            "                and i.INDEX_NAME = CONCAT(k.CONSTRAINT_NAME, '_idx')\n" +
            "                and i.COLUMN_NAME = k.COLUMN_NAME) CONSTRAINT_NAME\n" +
            "      from INFORMATION_SCHEMA.STATISTICS i\n" +
            "\n" +
            "      where i.TABLE_SCHEMA = '%s'\n" +
            "        and i.TABLE_NAME %s\n" +
            "        and i.INDEX_NAME <> 'PRIMARY') t\n" +
            "where t.CONSTRAINT_NAME is null";


    private TapConnectionContext tapConnectionContext;
    private OceanbaseJdbcContext oceanbaseJdbcContext;

    public OceanbaseSchemaLoader(OceanbaseJdbcContext oceanbaseJdbcContext) {
        this.oceanbaseJdbcContext = oceanbaseJdbcContext;
        this.tapConnectionContext = oceanbaseJdbcContext.getTapConnectionContext();
    }

    public void discoverSchema(List<String> filterTable, Consumer<List<TapTable>> consumer, int tableSize) {
        if (null == consumer) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }

        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");

        List<String> allTables = queryAllTables(database, filterTable);
        if (CollectionUtils.isEmpty(allTables)) {
            consumer.accept(null);
            return;
        }

        TableFieldTypesGenerator instance = InstanceFactory.instance(TableFieldTypesGenerator.class);
        DefaultExpressionMatchingMap dataTypesMap = tapConnectionContext.getSpecification().getDataTypesMap();

        try {
            List<List<String>> partition = Lists.partition(allTables, tableSize);
            partition.forEach(tables -> {
                String tableNames = StringUtils.join(tables, "','");
                List<DataMap> columnList = queryAllColumns(database, tableNames);
                List<DataMap> indexList = queryAllIndexes(database, tableNames);

                Map<String, List<DataMap>> columnMap = Maps.newHashMap();
                if (CollectionUtils.isNotEmpty(columnList)) {
                    columnMap = columnList.stream().collect(Collectors.groupingBy(t -> t.getString("TABLE_NAME")));
                }
                Map<String, List<DataMap>> indexMap = Maps.newHashMap();
                if (CollectionUtils.isNotEmpty(indexList)) {
                    indexMap = indexList.stream().collect(Collectors.groupingBy(t -> t.getString("TABLE_NAME")));
                }

                Map<String, List<DataMap>> finalColumnMap = columnMap;
                Map<String, List<DataMap>> finalIndexMap = indexMap;

                List<TapTable> tempList = new ArrayList<>();
                tables.forEach(table -> {
                    TapTable tapTable = TapSimplify.table(table);

                    discoverFields(finalColumnMap.get(table), tapTable, instance, dataTypesMap);
                    discoverIndexes(finalIndexMap.get(table), tapTable);
                    tempList.add(tapTable);
                });

                if (CollectionUtils.isNotEmpty(columnList)) {
                    consumer.accept(tempList);
                    tempList.clear();
                }
            });

        } catch (Exception e) {
            throw new RuntimeException(e);
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
            oceanbaseJdbcContext.query(sql, resultSet -> {
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
        String sql = String.format(SELECT_ALL_INDEX_SQL, database, database, inTableName);
        try {
            oceanbaseJdbcContext.query(sql, resultSet -> {
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

    private void discoverIndexes(List<DataMap> indexList, TapTable tapTable) {
        List<TapIndex> indexes = new ArrayList<>();

        if (CollectionUtils.isEmpty(indexList)) {
            return;
        }
        indexList.forEach(dataMap -> {
            String indexName = dataMap.getString("INDEX_NAME");
            TapIndex tapIndex = indexes.stream().filter(i -> i.getName().equals(indexName)).findFirst().orElse(null);
            if (null == tapIndex) {
                tapIndex = new TapIndex();
                tapIndex.setName(indexName);
                int nonUnique = Integer.parseInt(dataMap.getString("NON_UNIQUE"));
                tapIndex.setUnique(nonUnique == 1);
                tapIndex.setPrimary(false);
                indexes.add(tapIndex);
            }
            List<TapIndexField> indexFields = tapIndex.getIndexFields();
            if (null == indexFields) {
                indexFields = new ArrayList<>();
                tapIndex.setIndexFields(indexFields);
            }
            TapIndexField tapIndexField = new TapIndexField();
            tapIndexField.setName(dataMap.getString("COLUMN_NAME"));
            String collation = dataMap.getString("COLLATION");
            tapIndexField.setFieldAsc("A".equals(collation));
            indexFields.add(tapIndexField);
        });
        tapTable.setIndexList(indexes);

    }

    public List<String> queryAllTables(String database, List<String> filterTable) {
        String sql = String.format(SELECT_TABLES, database);
        if (CollectionUtils.isNotEmpty(filterTable)) {
            filterTable = filterTable.stream().map(t -> "'" + t + "'").collect(Collectors.toList());
            String tableNameIn = String.join(",", filterTable);
            sql += String.format(TABLE_NAME_IN, tableNameIn);
        }

        List<String> tableList = TapSimplify.list();
        try {
            oceanbaseJdbcContext.query(sql, resultSet -> {
                while (resultSet.next()) {
                    tableList.add(resultSet.getString("TABLE_NAME"));
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }
}
