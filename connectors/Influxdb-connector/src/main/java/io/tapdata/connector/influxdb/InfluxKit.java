package io.tapdata.connector.influxdb;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InfluxKit {
    public static List<String> getColumnsFromResultSet(QueryResult resultSet) {
        //get all column names
        List<String> columnNames = new ArrayList<>();
    
        List<QueryResult.Result> resultSetMetaData = resultSet.getResults();
        for (QueryResult.Result result: resultSetMetaData) {
            List<QueryResult.Series> columnNameArr = result.getSeries();
            List<String> columns =  columnNameArr.stream().map(QueryResult.Series::getName).collect(Collectors.toList());
            columnNames.addAll(columns);
        }
        return columnNames;
    }
    
    /**
     * 获取数据索引
     * @param resultSet
     * @param columnNames
     * @return
     */
    public static DataMap getIndexesFromResultSet(QueryResult resultSet, Collection<String> columnNames) {
        DataMap map = DataMap.create();
        
    
        return null;
    }
    
    /**
     * 获取数据行结构
     * @param resultSet
     * @param columnNames
     * @return
     */
    public static DataMap getRowFromResultSet(QueryResult resultSet, Collection<String> columnNames) {
        DataMap map = DataMap.create();
        Map<List<String>, List<List<Object>>> query = new HashMap<>();
        Map<String, List<Object>> data = new HashMap<>();
        
        List<List<QueryResult.Series>> result =  resultSet.getResults().stream().map(QueryResult.Result::getSeries).collect(Collectors.toList());
        if (EmptyKit.isNotNull(resultSet) && resultSet.getResults().size() > 0) {
            
            for(List<QueryResult.Series> series: result ){
                for (int i=0; i< series.size(); i++){
                    QueryResult.Series series1 = series.get(0);
                    query.put(series1.getColumns(), series1.getValues());
                }
            }
            
            for (Map.Entry<List<String>, List<List<Object>>> item: query.entrySet()){
                for(int i=0;i< item.getKey().size();i++){
                    String column = item.getKey().get(i);
                    List<Object> values = item.getValue().get(i);
                    data.put(column, values);
                }
                
            }
            
            for (String col : columnNames) {
                map.put(col,data.get(col));
            }
            return map;
        }
        return null;
    }
    
    
}
