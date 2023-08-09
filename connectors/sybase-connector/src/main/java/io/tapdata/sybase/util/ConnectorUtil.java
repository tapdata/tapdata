package io.tapdata.sybase.util;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.sybase.SybaseConnector;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;
import io.tapdata.sybase.extend.SybaseContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

/**
 * @author GavinXiao
 * @description ConnectorUtil create by Gavin
 * @create 2023/8/8 14:35
 **/
public class ConnectorUtil {

    public static Set<String> dateFields(TapTable tapTable) {
        Set<String> dateTypeSet = new HashSet<>();
        tapTable.getNameFieldMap().forEach((n, v) -> {
            switch (v.getTapType().getType()) {
                case TapType.TYPE_DATE:
                case TapType.TYPE_DATETIME:
                    dateTypeSet.add(n);
                    break;
                default:
                    break;
            }
        });
        return dateTypeSet;
    }


    public static boolean equalsTable(List<String> cdcTables, Set<String> tables) {
        if (null == cdcTables || cdcTables.isEmpty()) return false;
        for (String table : tables) {
            if (!cdcTables.contains(table)) return false;
        }
        return true;
    }



    public static TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName, SybaseContext sybaseContext) {
        DataMap dataMap = sybaseContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
    }


    public static void makePrimaryKeyAndIndex(List<DataMap> indexList, String table, Set<String> primaryKey, List<TapIndex> tapIndexList) {
        Map<String, List<DataMap>> indexMap = indexList.stream().filter(idx -> table.equals(idx.getString("tableName")))
                .collect(Collectors.groupingBy(idx -> idx.getString("index_name"), LinkedHashMap::new, Collectors.toList()));
        indexMap.forEach((key, value) -> {
            if (value.stream().anyMatch(v -> ("clustered, unique".equals(v.getString("index_description"))))) {
                primaryKey.addAll(value.stream().filter(v -> Objects.nonNull(v) && ("clustered, unique".equals(v.getString("index_description")))).filter(v -> null != v.get("index_keys")).map(v -> v.getString("index_keys")).collect(Collectors.toList()));
            }
            tapIndexList.add(makeTapIndex(key, value));
        });
    }

    public static TapIndex makeTapIndex(String key, List<DataMap> value) {
        TapIndex index = new TapIndex();
        index.setName(key);
        value.forEach(v -> {
            String indexKeys = v.getString("index_keys");
            String[] keyNames = indexKeys.split(",");
            String indexDescription = v.getString("index_description");
            List<TapIndexField> fieldList = TapSimplify.list();
            for (String keyName : keyNames) {
                if (null == keyName || "".equals(keyName.trim())) continue;
                TapIndexField field = new TapIndexField();
                //field.setFieldAsc("1".equals(v.getString("isAsc")));
                field.setName(keyName.trim());
                fieldList.add(field);
            }
            index.setUnique(indexDescription.contains("unique"));
            index.setPrimary(indexDescription.contains("clustered, unique"));
            index.setIndexFields(fieldList);

        });
        return index;
    }

    public static synchronized List<String> getOutTableList(CopyOnWriteArraySet<List<String>> tableLists) {
        if (EmptyKit.isNotEmpty(tableLists)) {
            List<String> list = tableLists.stream().findFirst().orElseGet(ArrayList::new);
            tableLists.remove(list);
            return list;
        }
        return null;
    }

    public static String maintenanceTaskId(TapConnectorContext tapConnectionContext) {
        KVMap<Object> stateMap = tapConnectionContext.getStateMap();
        Object taskId = stateMap.get("taskId");
        if (null == taskId) {
            String id = tapConnectionContext.getId();
            if (null == id) {
                id = UUID.randomUUID().toString().replaceAll("-", "_");
            }
            stateMap.put("taskId", id.substring(0, 15));
        }
        return (String) taskId;
    }

    public static void maintenanceTaskIdInGlobalStateMap(String taskId, TapConnectorContext context) {
        KVMap<Object> globalStateMap = context.getGlobalStateMap();
        synchronized (SybaseConnector.filterConfigLock){
            Object aliveStreamTask = globalStateMap.get("aliveStreamTask");
            if (!(aliveStreamTask instanceof Set)) {
                aliveStreamTask = new HashSet<String>();
            }
            ((Set<String>) aliveStreamTask).add(taskId);
            globalStateMap.put("aliveStreamTask", aliveStreamTask);
        }
    }

    public static Set<String> removeTaskIdInGlobalStateMap(String taskId, TapConnectorContext context) {
        KVMap<Object> globalStateMap = context.getGlobalStateMap();
        Object aliveStreamTask = null;
        synchronized (SybaseConnector.filterConfigLock){
            aliveStreamTask = globalStateMap.get("aliveStreamTask");
            if (aliveStreamTask instanceof Set) {
                Set<String> streamTask = (Set<String>) aliveStreamTask;
                if (streamTask.contains(taskId)) {
                    streamTask.remove(taskId);
                    globalStateMap.put("aliveStreamTask", aliveStreamTask);
                }
            }
        }
        return (Set<String>) aliveStreamTask;
    }

    public static List<Map<String, Object>> maintenanceCdcMonitorTableMap(List<Map<String, Object>> sybaseFilters, TapConnectorContext tapConnectionContext) {
        KVMap<Object> globalStateMap = tapConnectionContext.getGlobalStateMap();
        globalStateMap.put("CdcMonitorTableMap", sybaseFilters);
        return sybaseFilters;
    }

    public static void removeCdcMonitorTableMap(TapConnectorContext tapConnectionContext) {
        KVMap<Object> globalStateMap = tapConnectionContext.getGlobalStateMap();
        globalStateMap.remove("CdcMonitorTableMap");
    }

    public static OverwriteType maintenanceTableOverType(TapConnectorContext tapConnectionContext) {
        OverwriteType tableOverType = OverwriteType.type(String.valueOf(tapConnectionContext.getGlobalStateMap().get("tableOverType")));
        tapConnectionContext.getGlobalStateMap().put("tableOverType", tableOverType.getType());
        return tableOverType;
    }

    public static Set<String> getAllTableFromTask(TapConnectorContext tapConnectionContext) {
        Iterator<Entry<TapTable>> tableIterator = tapConnectionContext.getTableMap().iterator();
        Set<String> tableIds = new HashSet<>();
        while (tableIterator.hasNext()) {
            Entry<TapTable> next = tableIterator.next();
            if (null != next) {
                String key = next.getKey();
                if (null != key && !"".equals(key.trim())) {
                    tableIds.add(key);
                }
            }
        }
        tapConnectionContext.getLog().info("Task table will be monitor in cdc: {}", tableIds);
        return tableIds;
    }

    public static Set<String> containsTimestampFieldTables(Set<String> tableIds, SybaseContext sybaseContext) {
        final Set<String> containsTimestampFieldTables = new HashSet<>();
        try {
            List<DataMap> tableList = sybaseContext.queryAllTables(new ArrayList<>(tableIds));
            Map<String, List<DataMap>> tableName = tableList.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(t -> t.getString("tableName")));
            if (null != tableName && !tableName.isEmpty()) {
                tableName.forEach((tab, con) -> {
                    if (null != con) {
                        List<DataMap> collect = con.stream().filter(col -> null != col
                                && null != col.getString("dataType")
                                && col.getString("dataType").toUpperCase(Locale.ROOT).contains("TIMESTAMP"))
                                .collect(Collectors.toList());
                        if (!collect.isEmpty()) {
                            containsTimestampFieldTables.add(tab);
                        }
                    }
                });
            }

        } catch (Exception e) {
            throw new CoreException("Can not get any tables from sybase, filter by: {}, msg: {}", tableIds, e.getMessage());
        }
        return containsTimestampFieldTables;
    }
}
