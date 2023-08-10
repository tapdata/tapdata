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
import io.tapdata.sybase.cdc.dto.start.SybaseFilterConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseReInitConfig;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.SybaseContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    /**
     * 从表模型中获取date和 datetime 类型的字段
     */
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

    /**
     * 对比两个集合中表是否有变化
     */
    public static boolean equalsTable(List<String> cdcTables, Set<String> tables) {
        if (null == cdcTables || cdcTables.isEmpty()) return false;
        for (String table : tables) {
            if (!cdcTables.contains(table)) return false;
        }
        return true;
    }

    /**
     * 查询获取表信息
     */
    public static TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName, SybaseContext sybaseContext) {
        DataMap dataMap = sybaseContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
    }

    /**
     * 获取表索引
     */
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

    /**
     * 获取表索引
     */
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

    /**
     * 移除第一张表
     */
    public static synchronized List<String> getOutTableList(CopyOnWriteArraySet<List<String>> tableLists) {
        if (EmptyKit.isNotEmpty(tableLists)) {
            List<String> list = tableLists.stream().findFirst().orElseGet(ArrayList::new);
            tableLists.remove(list);
            return list;
        }
        return null;
    }

    /**
     * 在StateMap中维护任务id
     */
    public static String maintenanceTaskId(TapConnectorContext tapConnectionContext) {
        KVMap<Object> stateMap = tapConnectionContext.getStateMap();
        Object taskId = stateMap.get("taskId");
        if (null == taskId) {
            taskId = tapConnectionContext.getId();
            if (null == taskId) {
                taskId = UUID.randomUUID().toString().replaceAll("-", "_");
            }
            stateMap.put("taskId", taskId);//.substring(0, 15));
        }
        return (String) taskId;
    }

    /**
     * 在GlobalStateMap中维护任务正在进行cdc的任务ID列表
     */
    public static void maintenanceTaskIdInGlobalStateMap(String taskId, TapConnectorContext context) {
        KVMap<Object> globalStateMap = context.getGlobalStateMap();
        synchronized (SybaseConnector.filterConfigLock) {
            Object aliveStreamTask = globalStateMap.get("aliveStreamTask");
            if (!(aliveStreamTask instanceof Set)) {
                aliveStreamTask = new HashSet<String>();
            }
            ((Set<String>) aliveStreamTask).add(taskId);
            globalStateMap.put("aliveStreamTask", aliveStreamTask);
        }
    }

    /**
     * 把当前任务从GlobalStateMap的任务正在进行cdc的任务ID列表中移除
     */
    public static Set<String> removeTaskIdInGlobalStateMap(String taskId, TapConnectorContext context) {
        KVMap<Object> globalStateMap = context.getGlobalStateMap();
        Object aliveStreamTask = null;
        synchronized (SybaseConnector.filterConfigLock) {
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

    /**
     * 在GlobalStateMap中维护任务正在被cdc监听的表
     */
    public static List<Map<String, Object>> maintenanceCdcMonitorTableMap(List<Map<String, Object>> sybaseFilters, TapConnectorContext tapConnectionContext) {
        KVMap<Object> globalStateMap = tapConnectionContext.getGlobalStateMap();
        globalStateMap.put("CdcMonitorTableMap", sybaseFilters);
        return sybaseFilters;
    }

    public static Set<String> getTableFroMaintenanceCdcMonitorTableMap(TapConnectorContext tapConnectorContext, ConnectionConfig config) {
        KVMap<Object> globalStateMap = tapConnectorContext.getGlobalStateMap();
        Object cdcMonitorTableSet = globalStateMap.get("CdcMonitorTableMap");
        List<Map<String, Object>> monitorTableSet = (List<Map<String, Object>>) cdcMonitorTableSet;
        config = Optional.ofNullable(config).orElse(new ConnectionConfig(tapConnectorContext));
        String database = config.getDatabase();
        String schema = config.getSchema();
        Set<String> tableSet = new HashSet<>();
        if (null != monitorTableSet && !monitorTableSet.isEmpty()) {
            monitorTableSet.stream().filter(Objects::nonNull).forEach(tableInfoMap -> {
                Object catalogName = tableInfoMap.get("catalog");
                Object schemaName = tableInfoMap.get("schema");
                if (null != catalogName && catalogName.equals(database) && null != schemaName && schemaName.equals(schema)) {
                    Object allowObj = tableInfoMap.get(SybaseFilterConfig.configKey);
                    if (allowObj instanceof Collection) {
                        Collection<Object> allowList = (Collection<Object>) allowObj;
                        if (!allowList.isEmpty()) {
                            allowList.stream().filter(item -> Objects.nonNull(item) && item instanceof Map).forEach(tabInfo -> {
                                Map<String, Object> tableMap = (Map<String, Object>) tabInfo;
                                tableSet.addAll(tableMap.keySet());
                            });
                        }
                    }
                }
            });
        }
        return tableSet;
    }

    /**
     * 清空GlobalStateMap中维护任务正在被cdc监听的表
     */
    public static void removeCdcMonitorTableMap(TapConnectorContext tapConnectionContext) {
        KVMap<Object> globalStateMap = tapConnectionContext.getGlobalStateMap();
        globalStateMap.remove("CdcMonitorTableMap");
    }

    /**
     * 在GlobalStateMap中维护任务正在执行的CDC命令 类型
     */
    public static OverwriteType maintenanceTableOverType(TapConnectorContext tapConnectionContext) {
        OverwriteType tableOverType = OverwriteType.type(String.valueOf(tapConnectionContext.getGlobalStateMap().get("tableOverType")));
        tapConnectionContext.getGlobalStateMap().put("tableOverType", tableOverType.getType());
        return tableOverType;
    }

    /**
     * 获取任务里加载的所有表
     */
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

    /**
     * 通过查询获取表是否包含timestamp字段，并返回包含这个字段的表
     */
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

    /**
     * 在globalStateMap中维护一个唯一的cdc进程ID
     */
    public synchronized static String maintenanceGlobalCdcProcessId(TapConnectorContext tapConnectionContext) {
        KVMap<Object> globalStateMap = tapConnectionContext.getGlobalStateMap();
        Object globalSybaseCdcProcessId = globalStateMap.get("globalSybaseCdcProcessId");
        if (!(globalSybaseCdcProcessId instanceof String)) {
            globalStateMap.put("globalSybaseCdcProcessId", globalSybaseCdcProcessId = UUID.randomUUID().toString().replaceAll("-", "_").substring(0, 15));
        }
        return (String) globalSybaseCdcProcessId;
    }

    /**
     * 把globalStateMap中维护的一个唯一的cdc进程ID重置
     */
    public synchronized static void removeGlobalCdcProcessId(TapConnectorContext tapConnectionContext) {
        KVMap<Object> globalStateMap = tapConnectionContext.getGlobalStateMap();
        globalStateMap.remove("globalSybaseCdcProcessId");
    }

    public static List<Map<String, Object>> fixYaml(List<SybaseFilterConfig> configs) {
        List<Map<String, Object>> list = new ArrayList<>();
        if (null == configs || configs.isEmpty()) return list;
        configs.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(SybaseFilterConfig::getCatalog)).forEach((cl, r) -> {
            r.stream().collect(Collectors.groupingBy(SybaseFilterConfig::getSchema)).forEach((s, ri) -> {
                LinkedHashMap<String, Object> map = new LinkedHashMap<>();
                map.put("catalog", cl);
                map.put("schema", s);
                if (ri == null) ri = new ArrayList<>();
                map.put("types", ri.isEmpty() ? null : ri.get(0).getTypes());
                Map<String, Object> tab = new HashMap<>();
                for (SybaseFilterConfig config : ri) {
                    tab.putAll(config.getAllow());
                }
                map.put("allow", tab);
                list.add(map);
            });
        });
        return list;
    }

    public static List<LinkedHashMap<String, Object>> fixYaml0(List<SybaseReInitConfig> configs) {
        List<LinkedHashMap<String, Object>> list = new ArrayList<>();
        if (null == configs || configs.isEmpty()) return list;
        configs.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(SybaseReInitConfig::getCatalog)).forEach((cl, r) -> {
            r.stream().collect(Collectors.groupingBy(SybaseReInitConfig::getSchema)).forEach((s, ri) -> {
                LinkedHashMap<String, Object> map = new LinkedHashMap<>();
                map.put("catalog", cl);
                map.put("schema", s);
                Set<String> tab = new HashSet<>();
                for (SybaseReInitConfig sybaseReInitConfig : ri) {
                    tab.addAll(sybaseReInitConfig.getAdd_tables());
                }
                map.put("add-tables", tab);
                list.add(map);
            });
        });
        return list;
    }

    public static List<SybaseFilterConfig> fromYaml(List<Map<String, Object>> mapList) {
        List<SybaseFilterConfig> list = new ArrayList<>();
        if (null != mapList && !mapList.isEmpty()) {
            for (Map<String, Object> map : mapList) {
                SybaseFilterConfig config = new SybaseFilterConfig();
                config.setCatalog(((String) map.get("catalog")));
                config.setSchema(((String) map.get("schema")));
                config.setTypes(((List<String>) map.get("types")));
                config.setAllow((Map<String, Object>) map.get("allow"));
            }
        }
        return list;
    }

//    public static final Map<String, List<String>> ignoreColumns = new HashMap<String, List<String>>() {{
//        put("block", new ArrayList<String>() {{
//            add("timestamp");
//        }});
//    }};
//    public static final Map<String, List<String>> unIgnoreColumns = new HashMap<String, List<String>>() {{
//        put("block", new ArrayList<String>());
//    }};

    public static Map<String, List<String>> unIgnoreColumns() {
        Map<String, List<String>> hashMap = new HashMap<>();
        hashMap.put("block", new ArrayList<String>());
        return  hashMap;
    }

    public static Map<String, List<String>> ignoreColumns() {
        Map<String, List<String>> hashMap = new HashMap<>();
        List<String> timestamp = new ArrayList<>();
        timestamp.add("timestamp");
        hashMap.put("block", timestamp);
        return hashMap;
    }
}
