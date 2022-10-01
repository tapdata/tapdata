package io.tapdata.connector.influxdb;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.deploy.util.StringUtils;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.collections4.CollectionUtils;
import org.influxdb.InfluxDB;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class InfluxdbSchemaLoader {
    private static final String TAG = InfluxdbSchemaLoader.class.getSimpleName();
    private TapConnectionContext tapConnectionContext;
    private InfluxdbContext influxdbContext;
    
    private static final String SELECT_TABLES = "show measurements";
    private static final String TABLE_NAME_IN = " AND TABLE_NAME IN(%s)";
    
    private static final String SELECT_COLUMNS = "";
    
    public InfluxdbSchemaLoader(InfluxdbContext influxdbContext){
        this.influxdbContext = influxdbContext;
        this.tapConnectionContext = influxdbContext.getTapConnectionContext();
    }
    
    public void discoverSchema(final TapConnectionContext tapConnectionContext, final InfluxdbConfig influxdbConfig, List<String> filterTable, Consumer<List<TapTable>> consumer, int tableSize) throws Throwable {
        if (null == consumer) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }
        
        String database = influxdbConfig.getDatabase();
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
                    
                    //TODO 加载表和字段
                    tempList.add(tapTable);
                });
                
                if (CollectionUtils.isNotEmpty(columnList)) {
                    consumer.accept(tempList);
                    tempList.clear();
                }
            });
        } catch (Exception e) {
            throw new Exception(e);
        }
    }
    
    private List<DataMap> queryAllIndexes(String database, String tableNames) {
        TapLogger.debug(TAG, "Query all indexes, database: {}, tableNames:{}", database, tableNames);
        List<DataMap> indexList = TapSimplify.list();
        
        String inTableName = new StringJoiner(tableNames).add("IN ('").add("')").toString();
        String sql = String.format("", database, database, inTableName);
        
        try {
            QueryResult resultSet = influxdbContext.executeQuery(sql);
            List<String> columnNames = InfluxKit.getColumnsFromResultSet(resultSet);
            for (QueryResult.Result result: resultSet.getResults()) {
                indexList.add(InfluxKit.getIndexesFromResultSet(resultSet, columnNames));
                
            }
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }
    
    public List<String> queryAllTables(String database, final List<String> filterTables) {
        final List<String> tableList = TapSimplify.list();
        final InfluxDB connection = influxdbContext.getConnection();
        try {
             final QueryResult resultSet = queryTables(database, filterTables);
            for (QueryResult.Result result: resultSet.getResults()) {
                for (QueryResult.Series series: result.getSeries()){
                    tableList.addAll(series.getTags().keySet());
                }
            }
        } catch (final Exception e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        
        return tableList;
    }
    
    private List<DataMap> queryAllColumns(String database, String tableNames) {
        TapLogger.debug(TAG, "Query all columns, database: {}, tableNames:{}", database, tableNames);
        
        String inTableName = new StringJoiner(tableNames).add("IN ('").add("')").toString();
        String sql = String.format(SELECT_COLUMNS, database, inTableName);
        List<DataMap> columnList = TapSimplify.list();
       
        final QueryResult queryResult = influxdbContext.executeQuery(sql);
            List<String> columnNames = InfluxKit.getColumnsFromResultSet(queryResult);
            for (QueryResult.Result result: queryResult.getResults()) {
                //从Influxdb获取字段 解析出schema
                columnList.add(InfluxKit.getRowFromResultSet(queryResult, columnNames));
            }
       
        return columnList;
    }
    
    public QueryResult queryTables(String database, final List<String> filterTables) throws Exception {
        String sql = String.format(SELECT_TABLES, database);
        if (CollectionUtils.isNotEmpty(filterTables)) {
            final List<String> wrappedTables = filterTables.stream().map(t -> "'" + t + "'").collect(Collectors.toList());
            String tableNameIn = String.join(",", wrappedTables);
            sql += String.format(TABLE_NAME_IN, tableNameIn);
        }
        TapLogger.debug(TAG, "Execute sql: " + sql);
        return influxdbContext.executeQuery(sql);
    }
    
    
    
}
