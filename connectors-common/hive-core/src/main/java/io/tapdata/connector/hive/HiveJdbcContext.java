package io.tapdata.connector.hive;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    public Map<String, Map<String, Object>> queryTablesDesc(List<String> tableNames) throws SQLException {
        Map<String, Map<String, Object>> tableMap = new HashMap<>();
        for (String table : tableNames) {
            query(String.format(HIVE_TABLE_DESC, table),
                    resultSet -> tableMap.put(table, getTableInfo(resultSet)));
        }
        return tableMap;
    }

    public Map<String, Object> getTableInfo(ResultSet resultSet) throws SQLException {
        Map<String, Object> result = new HashMap<>();
        Map<String, String> detailTableInfo = new HashMap<>();
        Map<String, String> tableParams = new HashMap<>();
        Map<String, String> storageInfo = new HashMap<>();
        Map<String, String> storageDescParams = new HashMap<>();
        Map<String, Map<String, String>> constraints = new HashMap<>();
        List<Map<String, String>> columns = new ArrayList<>();
        List<Map<String, String>> partitions = new ArrayList<>();

        Map<String, String> moduleMap = getDescTableModule();

        String infoModule = "";
        while (resultSet.next()) {
            String title = resultSet.getString(1).trim();
            if (("".equals(title) && resultSet.getString(2) == null) || "# Constraints".equals(title)) {
                continue;
            }
            if (moduleMap.containsKey(title)) {
                if ("partition_info".equals(infoModule) && "col_name".equals(moduleMap.get(title))) {
                    continue;
                }
                infoModule = moduleMap.get(title);
                continue;
            }

            String key;
            String value;
            switch (infoModule) {
                case "col_name":
                    Map<String, String> map = new HashMap<>();
                    int colNum = resultSet.getMetaData().getColumnCount();
                    for (int col = 0; col < colNum; col++) {
                        String columnName = resultSet.getMetaData().getColumnName(col + 1);
                        String columnValue = resultSet.getString(columnName);
                        map.put(columnName, columnValue);
                    }
                    columns.add(map);
                    break;

                case "table_info":
                    key = resultSet.getString(1).trim().replace(":", "");
                    value = resultSet.getString(2).trim();
                    detailTableInfo.put(key, value);
                    break;

                case "table_param":
                    key = resultSet.getString(2).trim().replace(":", "");
                    value = resultSet.getString(3).trim();
                    tableParams.put(key, value);
                    break;

                case "storage_info":
                    key = resultSet.getString(1).trim().replace(":", "");
                    value = resultSet.getString(2).trim();
                    storageInfo.put(key, value);
                    break;

                case "storage_desc":
                    key = resultSet.getString(2).trim().replace(":", "");
                    value = resultSet.getString(3).trim();
                    storageDescParams.put(key, value);
                    break;

                case "not_null_constraint":
                    Map<String, String> notNullMap = constraints.getOrDefault("notnull", new HashMap<>());
                    if ("Table:".equals(title.trim())) resultSet.next();

                    String notNullConstraintName = resultSet.getString(2).trim();
                    resultSet.next();

                    key = resultSet.getString(2).trim();
                    notNullMap.put(key, notNullConstraintName);

                    constraints.put("notnull", notNullMap);
                    break;

                case "default_constraint":
                    Map<String, String> defaultMap = constraints.getOrDefault("default", new HashMap<>());
                    if ("Table:".equals(title.trim())) resultSet.next();

                    String defaultConstraintName = resultSet.getString(2).trim();
                    resultSet.next();

                    key = resultSet.getString(1).trim().split(":")[1];

                    defaultMap.put(key + "_constraintName", defaultConstraintName);

                    constraints.put("default", defaultMap);
                    break;

                case "partition_info":
                    Map<String, String> partitionMap = new HashMap<>();
                    int partitionColNum = resultSet.getMetaData().getColumnCount();
                    for (int col = 0; col < partitionColNum; col++) {
                        String columnName = resultSet.getMetaData().getColumnName(col + 1);
                        String columnValue = resultSet.getString(columnName);
                        partitionMap.put(columnName, columnValue);
                    }
                    partitions.add(partitionMap);
                    break;

                default:
                    System.out.print("unknown module,please update method to support it : " + infoModule);
            }

        }

        result.put("columns", columns);
        result.put("detailTableInfo", detailTableInfo);
        result.put("tableParams", tableParams);
        result.put("storageInfo", storageInfo);
        result.put("storageDescParams", storageDescParams);
        result.put("constraints", constraints);
        result.put("partitions", partitions);

        return result;
    }

    private static Map<String, String> getDescTableModule() {
        Map<String, String> descTableModule = new HashMap<>();

        descTableModule.put("# col_name", "col_name");
        descTableModule.put("# Detailed Table Information", "table_info");
        descTableModule.put("Table Parameters:", "table_param");
        descTableModule.put("# Storage Information", "storage_info");
        descTableModule.put("Storage Desc Params:", "storage_desc");
        descTableModule.put("# Not Null Constraints", "not_null_constraint");
        descTableModule.put("# Default Constraints", "default_constraint");
        descTableModule.put("# Partition Information", "partition_info");

        return descTableModule;
    }

    private final static String HIVE_ALL_TABLE = "show tables";

    private final static String HIVE_TABLE_DESC = "desc formatted `%s`";
}
