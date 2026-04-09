package com.tapdata.tm.task.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.group.vo.DagChangeDetail;
import com.tapdata.tm.group.vo.FieldChange;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.*;

/**
 * 任务配置对比工具类
 * 用于比较两个任务的配置是否一致，采用 JSON 序列化方式进行对比
 *
 * @author Tapdata
 */
@Slf4j
public class TaskConfigCompareUtil {

    private static final ObjectMapper OBJECT_MAPPER;

    /**
     * 需要对比的配置字段列表
     * 包含核心任务逻辑、运行参数与调优、告警与通知等配置
     */
    private static final List<String> CONFIG_FIELDS = Arrays.asList(
            // 核心任务逻辑 (Core Logic)
            "name",                         // 任务名称
            "type",                         // 任务类型
            "syncType",                     // 同步模式
            "deduplicWriteMode",            // 写入去重模式
            "dag",                          // 有向无环图
            "syncPoints",                   // 同步起始点

            // 运行参数与调优 (Runtime Settings & Tuning)
            "readBatchSize",                // 全量一批读取条数
            "writeBatchSize",               // 写入批量条数
            "writeBatchWaitMs",             // 写入每批最大等待时间
            "writeThreadSize",              // 目标写入线程数
            "processorThreadNum",           // 处理器线程数
            "increaseReadSize",             // 增量读取条数
            "increaseSyncInterval",         // 增量同步间隔
            "isStopOnError",                // 遇到错误是否停止
            "isAutoCreateIndex",            // 是否自动创建索引
            "dataSaving",                   // 是否保存数据/开启中间存储
            "isFilter",                     // 过滤设置
            "isOpenAutoDDL",                // 自动处理DDL

            // 增量同步配置 (CDC Settings)
            "shareCdcEnable",               // 共享挖掘开关

            // 调度配置 (Scheduling)
            "isSchedule",                   // 是否为调度任务
            "crontabExpression",            // crontab表达式
            "crontabExpressionFlag",        // crontab表达式开关

            // 高级配置 (Advanced Settings)
            "shareCache",                   // 共享缓存
            "isAutoInspect",                // 是否开启数据校验
            "skipErrorEvent",               // 跳过错误事件配置
            "doubleActive",                 // 是否开启双活
            "dynamicAdjustMemoryUsage",     // 动态调整队列内存
            "dynamicAdjustMemoryThresholdByte", // 动态调整阈值
            "dynamicAdjustMemorySampleRate",    // 采样比例
            "autoIncrementalBatchSize",      // 自动调整增量批次大小
            "enableSyncMetricCollector",
            "fileLog",
            "needFilterEventData"

    );

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        OBJECT_MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private TaskConfigCompareUtil() {
        // 私有构造函数，防止实例化
    }

    /**
     * 比较两个任务的配置是否一致
     *
     * @param importTask   导入的任务
     * @param existingTask 现有的任务
     * @return true 表示配置一致，false 表示配置不一致
     */
    public static boolean isConfigEqual(TaskDto importTask, TaskDto existingTask) {
        if (importTask == null && existingTask == null) {
            return true;
        }
        if (importTask == null || existingTask == null) {
            return false;
        }

        try {
            Map<String, Object> importConfig = extractConfigFields(importTask);
            Map<String, Object> existingConfig = extractConfigFields(existingTask);

            String importJson = OBJECT_MAPPER.writeValueAsString(importConfig);
            String existingJson = OBJECT_MAPPER.writeValueAsString(existingConfig);

            boolean isEqual = importJson.equals(existingJson);
            if (!isEqual && log.isDebugEnabled()) {
                log.debug("Task config not equal. Import: {}, Existing: {}", importJson, existingJson);
            }
            return isEqual;
        } catch (JsonProcessingException e) {
            log.error("Failed to compare task config", e);
            return false;
        }
    }

    /**
     * 比较两个任务的配置，返回不一致的字段列表
     *
     * @param importTask   导入的任务
     * @param existingTask 现有的任务
     * @return 不一致的字段名称列表，如果配置一致则返回空列表
     */
    public static List<String> getDifferentFields(TaskDto importTask, TaskDto existingTask) {
        List<String> differentFields = new ArrayList<>();

        if (importTask == null && existingTask == null) {
            return differentFields;
        }
        if (importTask == null || existingTask == null) {
            return new ArrayList<>(CONFIG_FIELDS);
        }

        try {
            Map<String, Object> importConfig = extractConfigFields(importTask);
            Map<String, Object> existingConfig = extractConfigFields(existingTask);

            for (String field : CONFIG_FIELDS) {
                Object importValue = importConfig.get(field);
                Object existingValue = existingConfig.get(field);

                if (!isFieldEqual(importValue, existingValue)) {
                    differentFields.add(field);
                }
            }
        } catch (Exception e) {
            log.error("Failed to get different fields", e);
        }

        return differentFields;
    }

    /**
     * 从任务中提取需要对比的配置字段
     *
     * @param task 任务对象
     * @return 配置字段的 Map
     */
    private static Map<String, Object> extractConfigFields(TaskDto task) {
        Map<String, Object> config = new LinkedHashMap<>();

        // 核心任务逻辑
        config.put("name", task.getName());
        config.put("type", task.getType());
        config.put("syncType", task.getSyncType());
        config.put("deduplicWriteMode", task.getDeduplicWriteMode());
        config.put("dag", normalizeDagForComparison(task.getDag()));
        config.put("syncPoints", task.getSyncPoints());

        // 运行参数与调优
        config.put("readBatchSize", task.getReadBatchSize());
        config.put("writeBatchSize", task.getWriteBatchSize());
        config.put("writeBatchWaitMs", task.getWriteBatchWaitMs());
        config.put("writeThreadSize", task.getWriteThreadSize());
        config.put("processorThreadNum", task.getProcessorThreadNum());
        config.put("increaseReadSize", task.getIncreaseReadSize());
        config.put("increaseSyncInterval", task.getIncreaseSyncInterval());
        config.put("isStopOnError", task.getIsStopOnError());
        config.put("isAutoCreateIndex", task.getIsAutoCreateIndex());
        config.put("dataSaving", task.getDataSaving());
        config.put("isFilter", task.getIsFilter());
        config.put("isOpenAutoDDL", task.getIsOpenAutoDDL());

        // 增量同步配置
        config.put("shareCdcEnable", task.getShareCdcEnable());

        // 调度配置
        config.put("isSchedule", task.getIsSchedule());
        config.put("crontabExpression", task.getCrontabExpression());
        config.put("crontabExpressionFlag", task.getCrontabExpressionFlag());

        // 告警与通知
        config.put("alarmRules", task.getAlarmRules());
        config.put("alarmSettings", task.getAlarmSettings());
        config.put("emailReceivers", task.getEmailReceivers());
        config.put("notifyTypes", task.getNotifyTypes());

        // 高级配置
        config.put("shareCache", task.getShareCache());
        config.put("isAutoInspect", task.getIsAutoInspect());
        config.put("skipErrorEvent", task.getSkipErrorEvent());
        config.put("doubleActive", task.getDoubleActive());
        config.put("dynamicAdjustMemoryUsage", task.getDynamicAdjustMemoryUsage());
        config.put("dynamicAdjustMemoryThresholdByte", task.getDynamicAdjustMemoryThresholdByte());
        config.put("dynamicAdjustMemorySampleRate", task.getDynamicAdjustMemorySampleRate());
        config.put("autoIncrementalBatchSize", task.getAutoIncrementalBatchSize());
        config.put("enableSyncMetricCollector", task.getEnableSyncMetricCollector());
        config.put("fileLog", task.getFileLog());
        config.put("needFilterEventData", task.getNeedFilterEventData());


        return config;
    }

    /**
     * 比较两个字段值是否相等
     *
     * @param value1 第一个值
     * @param value2 第二个值
     * @return true 表示相等，false 表示不相等
     */
    private static boolean isFieldEqual(Object value1, Object value2) {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null || value2 == null) {
            return false;
        }

        try {
            String json1 = OBJECT_MAPPER.writeValueAsString(value1);
            String json2 = OBJECT_MAPPER.writeValueAsString(value2);
            return json1.equals(json2);
        } catch (JsonProcessingException e) {
            log.error("Failed to compare field value", e);
            return false;
        }
    }

    /**
     * 获取需要对比的配置字段列表
     *
     * @return 配置字段列表的副本
     */
    public static List<String> getConfigFields() {
        return new ArrayList<>(CONFIG_FIELDS);
    }

    /**
     * 返回两个任务之间详细的字段级变更列表，同时填充 DAG 分类变更详情。
     * 每个 {@link FieldChange} 包含字段路径、DB 中的旧值（from）、导入文件中的新值（to）。
     * DAG 内部按节点展开，路径格式为 {@code dag.nodes.{nodeName}.{field}}。
     *
     * @param dagChangeDetailOut 可选输出参数，非 null 时会按类型分类填充 DAG 变更
     */
    public static List<FieldChange> getDetailedChanges(TaskDto importTask, TaskDto existingTask,
                                                       DagChangeDetail dagChangeDetailOut) {
        List<FieldChange> changes = new ArrayList<>();
        if (importTask == null || existingTask == null) return changes;
        try {
            Map<String, Object> importConfig = extractConfigFields(importTask);
            Map<String, Object> existingConfig = extractConfigFields(existingTask);
            for (String field : CONFIG_FIELDS) {
                if ("dag".equals(field)) continue; // DAG 单独展开
                Object importVal = importConfig.get(field);
                Object existingVal = existingConfig.get(field);
                if (!isFieldEqual(importVal, existingVal)) {
                    changes.add(new FieldChange(field, existingVal, importVal));
                }
            }
            List<FieldChange> dagChanges = getDagDetailedChanges(importTask.getDag(), existingTask.getDag());
            if (dagChangeDetailOut != null) {
                categorizeDagChanges(dagChanges, dagChangeDetailOut);
                // 未被分类的 DAG 变更（如整个 DAG 为 null 时 field="dag"）仍加入 changes
                for (FieldChange c : dagChanges) {
                    String f = c.getField();
                    if (!f.startsWith("dag.nodes.") && !f.startsWith("dag.edges.")) {
                        changes.add(c);
                    }
                }
            } else {
                changes.addAll(dagChanges);
            }
        } catch (Exception e) {
            log.error("Failed to get detailed task changes", e);
        }
        return changes;
    }

    /**
     * 将 DAG 变更列表按路径分类到 DagChangeDetail 的五个列表中。
     */
    private static void categorizeDagChanges(List<FieldChange> dagChanges, DagChangeDetail detail) {
        for (FieldChange change : dagChanges) {
            String path = change.getField();
            if (path.startsWith("dag.edges.")) {
                if (change.getFrom() == null) {
                    detail.getEdgeAdditions().add(change);
                } else if (change.getTo() == null) {
                    detail.getEdgeRemovals().add(change);
                }
            } else if (path.startsWith("dag.nodes.")) {
                String afterPrefix = path.substring("dag.nodes.".length());
                if (!afterPrefix.contains(".")) {
                    if (change.getFrom() == null) {
                        detail.getNodeAdditions().add(change);
                    } else if (change.getTo() == null) {
                        detail.getNodeRemovals().add(change);
                    }
                } else {
                    detail.getNodeConfigChanges().add(change);
                }
            }
        }
    }

    /**
     * 对两个 DAG 做节点级和连线级详细对比。
     * 节点按 id 匹配，使用白名单（@EqField 注解）提取字段。
     * 连线直接使用节点 ID 比对，输出路径格式为 {@code dag.edges.sourceId->targetId}。
     */
    @SuppressWarnings("unchecked")
    private static List<FieldChange> getDagDetailedChanges(DAG importDag, DAG existingDag) {
        List<FieldChange> changes = new ArrayList<>();
        if (importDag == null && existingDag == null) return changes;
        if (importDag == null || existingDag == null) {
            changes.add(new FieldChange("dag", existingDag, importDag));
            return changes;
        }

        // 提取白名单字段用于节点比对（按 id 索引）
        Map<String, Map<String, Object>> importNodesById = extractNodeEqFieldsById(importDag);
        Map<String, Map<String, Object>> existingNodesById = extractNodeEqFieldsById(existingDag);

        Set<String> allIds = new LinkedHashSet<>(importNodesById.keySet());
        allIds.addAll(existingNodesById.keySet());

        for (String nodeId : allIds) {
            Map<String, Object> iNode = importNodesById.get(nodeId);
            Map<String, Object> eNode = existingNodesById.get(nodeId);
            String prefix = "dag.nodes." + nodeId + ".";
            if (iNode == null) {
                changes.add(new FieldChange("dag.nodes." + nodeId, eNode, null));
            } else if (eNode == null) {
                changes.add(new FieldChange("dag.nodes." + nodeId, null, iNode));
            } else {
                Set<String> allFields = new LinkedHashSet<>(iNode.keySet());
                allFields.addAll(eNode.keySet());
                for (String f : allFields) {
                    compareAndAdd(changes, prefix + f, eNode.get(f), iNode.get(f));
                }
            }
        }

        // Edge 比对
        Set<String> importEdges = extractEdgeSet(importDag);
        Set<String> existingEdges = extractEdgeSet(existingDag);

        for (String edge : importEdges) {
            if (!existingEdges.contains(edge)) {
                changes.add(new FieldChange("dag.edges." + edge, null, edge));
            }
        }
        for (String edge : existingEdges) {
            if (!importEdges.contains(edge)) {
                changes.add(new FieldChange("dag.edges." + edge, edge, null));
            }
        }

        return changes;
    }

    /**
     * 从 DAG 的所有节点中，使用 @EqField 注解白名单提取字段，按 id 索引。
     * 额外包含 id、name 和 type 字段用于节点识别。
     */
    private static Map<String, Map<String, Object>> extractNodeEqFieldsById(DAG dag) {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        if (dag == null) return result;
        List<Node> nodes = dag.getNodes();
        if (nodes == null) return result;

        for (Node<?> node : nodes) {
            Map<String, Object> fields = extractEqFields(node);
            String id = node.getId();
            if (id != null) {
                result.putIfAbsent(id, fields);
            }
        }
        return result;
    }

    /**
     * 反射遍历节点对象的类继承链（至 Object.class 为止），
     * 只提取 @EqField 标注的字段值，加上 id、name 和 type 用于识别。
     * 返回 Map&lt;String, Object&gt;
     */
    static Map<String, Object> extractEqFields(Object node) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (node == null) return result;

        // 添加识别字段
        if (node instanceof Node) {
            Node<?> n = (Node<?>) node;
            result.put("id", n.getId());
            result.put("name", n.getName());
            result.put("type", n.getType());
        }

        Class<?> clazz = node.getClass();
        for (; clazz != null && clazz != Object.class; clazz = clazz.getSuperclass()) {
            for (Field field : clazz.getDeclaredFields()) {
                EqField annotation = field.getAnnotation(EqField.class);
                if (annotation != null) {
                    String fieldName = field.getName();
                    // 跳过已添加的识别字段
                    if ("id".equals(fieldName) || "name".equals(fieldName) || "type".equals(fieldName)) {
                        continue;
                    }
                    try {
                        field.setAccessible(true);
                        Object value = field.get(node);
                        if (value != null) {
                            result.put(fieldName, value);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to access field {} on {}: {}", fieldName, node.getClass().getSimpleName(), e.getMessage());
                    }
                }
            }
        }
        return result;
    }

    /**
     * 从 DAG 中提取 edge 集合，直接使用节点 ID，格式为 "sourceId->targetId"。
     */
    private static Set<String> extractEdgeSet(DAG dag) {
        Set<String> edgeSet = new LinkedHashSet<>();
        if (dag == null) return edgeSet;

        List<com.tapdata.tm.commons.dag.Edge> edges = dag.getEdges();
        if (edges != null) {
            for (com.tapdata.tm.commons.dag.Edge edge : edges) {
                edgeSet.add(edge.getSource() + "->" + edge.getTarget());
            }
        }
        return edgeSet;
    }

    private static void compareAndAdd(List<FieldChange> changes, String path, Object from, Object to) {
        try {
            String fJson = OBJECT_MAPPER.writeValueAsString(from);
            String tJson = OBJECT_MAPPER.writeValueAsString(to);
            if (!Objects.equals(fJson, tJson)) {
                if (from instanceof List && to instanceof List) {
                    compareListAndAdd(changes, path, (List<?>) from, (List<?>) to);
                } else {
                    changes.add(new FieldChange(path, from, to));
                }
            }
        } catch (JsonProcessingException e) {
            if (!Objects.equals(from, to)) changes.add(new FieldChange(path, from, to));
        }
    }

    /**
     * 对两个 List 进行元素级比对。
     * - 简单类型 (String, Number, Boolean) 列表：视为集合，报告 added/removed
     * - 复杂对象列表：先 JSON 完全匹配去重，再按标识字段匹配做字段级 diff
     */
    @SuppressWarnings("unchecked")
    private static void compareListAndAdd(List<FieldChange> changes, String path,
                                           List<?> fromList, List<?> toList) {
        if (fromList.isEmpty() && toList.isEmpty()) return;

        // Determine if this is a simple-type list
        boolean isSimple = isSimpleTypeList(fromList) && isSimpleTypeList(toList);

        if (isSimple) {
            compareSimpleListAndAdd(changes, path, fromList, toList);
        } else {
            compareComplexListAndAdd(changes, path, fromList, toList);
        }
    }

    private static boolean isSimpleTypeList(List<?> list) {
        if (list.isEmpty()) return true;
        Object first = list.get(0);
        return first instanceof String || first instanceof Number || first instanceof Boolean;
    }

    /**
     * 简单类型列表比对：视为集合，报告 added/removed 元素
     */
    private static void compareSimpleListAndAdd(List<FieldChange> changes, String path,
                                                 List<?> fromList, List<?> toList) {
        Set<String> fromSet = new LinkedHashSet<>();
        Set<String> toSet = new LinkedHashSet<>();
        for (Object o : fromList) fromSet.add(String.valueOf(o));
        for (Object o : toList) toSet.add(String.valueOf(o));

        List<String> removed = new ArrayList<>();
        for (String s : fromSet) {
            if (!toSet.contains(s)) removed.add(s);
        }
        List<String> added = new ArrayList<>();
        for (String s : toSet) {
            if (!fromSet.contains(s)) added.add(s);
        }

        if (!removed.isEmpty() || !added.isEmpty()) {
            changes.add(new FieldChange(path,
                    removed.isEmpty() ? null : removed,
                    added.isEmpty() ? null : added));
        }
    }

    /**
     * 复杂对象列表比对：先完全匹配去重，再按标识字段匹配做字段级 diff
     */
    @SuppressWarnings("unchecked")
    private static void compareComplexListAndAdd(List<FieldChange> changes, String path,
                                                  List<?> fromList, List<?> toList) {
        try {
            // Convert elements to Map representations
            List<Map<String, Object>> fromMaps = new ArrayList<>();
            List<Map<String, Object>> toMaps = new ArrayList<>();
            for (Object o : fromList) {
                fromMaps.add(OBJECT_MAPPER.convertValue(o, new TypeReference<Map<String, Object>>() {}));
            }
            for (Object o : toList) {
                toMaps.add(OBJECT_MAPPER.convertValue(o, new TypeReference<Map<String, Object>>() {}));
            }

            // Step 1: Remove exact matches (by JSON string)
            List<Map<String, Object>> unmatchedFrom = new ArrayList<>();
            List<Boolean> toMatched = new ArrayList<>();
            for (int i = 0; i < toMaps.size(); i++) toMatched.add(false);

            for (Map<String, Object> fMap : fromMaps) {
                String fJson = OBJECT_MAPPER.writeValueAsString(fMap);
                boolean found = false;
                for (int j = 0; j < toMaps.size(); j++) {
                    if (!toMatched.get(j)) {
                        String tJson = OBJECT_MAPPER.writeValueAsString(toMaps.get(j));
                        if (fJson.equals(tJson)) {
                            toMatched.set(j, true);
                            found = true;
                            break;
                        }
                    }
                }
                if (!found) unmatchedFrom.add(fMap);
            }

            List<Map<String, Object>> unmatchedTo = new ArrayList<>();
            for (int j = 0; j < toMaps.size(); j++) {
                if (!toMatched.get(j)) unmatchedTo.add(toMaps.get(j));
            }

            if (unmatchedFrom.isEmpty() && unmatchedTo.isEmpty()) return;

            // Step 2: Try matching by identity key
            Map<String, Map<String, Object>> fromByKey = new LinkedHashMap<>();
            Map<String, Map<String, Object>> toByKey = new LinkedHashMap<>();
            boolean keyUsable = true;

            for (Map<String, Object> m : unmatchedFrom) {
                String key = tryExtractKey(m);
                if (key == null || fromByKey.containsKey(key)) { keyUsable = false; break; }
                fromByKey.put(key, m);
            }
            if (keyUsable) {
                for (Map<String, Object> m : unmatchedTo) {
                    String key = tryExtractKey(m);
                    if (key == null || toByKey.containsKey(key)) { keyUsable = false; break; }
                    toByKey.put(key, m);
                }
            }

            if (keyUsable) {
                // Match by key and diff fields
                Set<String> allKeys = new LinkedHashSet<>(fromByKey.keySet());
                allKeys.addAll(toByKey.keySet());

                for (String key : allKeys) {
                    Map<String, Object> fMap = fromByKey.get(key);
                    Map<String, Object> tMap = toByKey.get(key);
                    String elemPath = path + "[" + key + "]";
                    if (fMap == null) {
                        changes.add(new FieldChange(elemPath, null, tMap));
                    } else if (tMap == null) {
                        changes.add(new FieldChange(elemPath, fMap, null));
                    } else {
                        // Field-level diff
                        Set<String> allFields = new LinkedHashSet<>(fMap.keySet());
                        allFields.addAll(tMap.keySet());
                        for (String f : allFields) {
                            compareAndAdd(changes, elemPath + "." + f, fMap.get(f), tMap.get(f));
                        }
                    }
                }
            } else {
                // Fallback: use index-based matching
                int maxLen = Math.max(unmatchedFrom.size(), unmatchedTo.size());
                for (int i = 0; i < maxLen; i++) {
                    Map<String, Object> fMap = i < unmatchedFrom.size() ? unmatchedFrom.get(i) : null;
                    Map<String, Object> tMap = i < unmatchedTo.size() ? unmatchedTo.get(i) : null;
                    String elemPath = path + "[" + i + "]";
                    if (fMap == null) {
                        changes.add(new FieldChange(elemPath, null, tMap));
                    } else if (tMap == null) {
                        changes.add(new FieldChange(elemPath, fMap, null));
                    } else {
                        Set<String> allFields = new LinkedHashSet<>(fMap.keySet());
                        allFields.addAll(tMap.keySet());
                        for (String f : allFields) {
                            compareAndAdd(changes, elemPath + "." + f, fMap.get(f), tMap.get(f));
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Fallback: record as whole-list change
            changes.add(new FieldChange(path, fromList, toList));
        }
    }

    /**
     * 尝试从 Map 中提取标识字段值，用于元素匹配
     */
    private static String tryExtractKey(Map<String, Object> element) {
        for (String keyField : Arrays.asList("id", "field", "tableName")) {
            Object val = element.get(keyField);
            if (val != null && !val.toString().isEmpty()) {
                return val.toString();
            }
        }
        return null;
    }

    /**
     * 对 DAG 进行规范化处理，使用白名单模式（@EqField 注解）提取节点字段，
     * 保留 edges（使用节点 ID），用于跨环境配置对比。
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeDagForComparison(DAG dag) {
        if (dag == null) {
            return null;
        }
        try {
            Map<String, Object> dagMap = new LinkedHashMap<>();

            // 使用白名单提取节点字段
            List<Map<String, Object>> normalizedNodes = new ArrayList<>();
            List<Node> nodes = dag.getNodes();
            if (nodes != null) {
                for (Node<?> node : nodes) {
                    normalizedNodes.add(extractEqFields(node));
                }
                // 按 id 排序保证稳定性
                normalizedNodes.sort(Comparator.comparing(
                        n -> n.getOrDefault("id", "").toString(),
                        Comparator.nullsFirst(Comparator.naturalOrder())
                ));
            }
            dagMap.put("nodes", normalizedNodes);

            // 规范化 edges：直接使用节点 ID，只保留 source/target
            List<Map<String, String>> normalizedEdges = new ArrayList<>();
            Set<String> edgeSet = extractEdgeSet(dag);
            for (String edge : edgeSet) {
                String[] parts = edge.split("->");
                if (parts.length == 2) {
                    Map<String, String> edgeMap = new LinkedHashMap<>();
                    edgeMap.put("source", parts[0]);
                    edgeMap.put("target", parts[1]);
                    normalizedEdges.add(edgeMap);
                }
            }
            normalizedEdges.sort(Comparator.comparing((Map<String, String> e) -> e.get("source"))
                    .thenComparing(e -> e.get("target")));
            dagMap.put("edges", normalizedEdges);

            return dagMap;
        } catch (Exception e) {
            log.warn("Failed to normalize DAG for comparison, fallback to raw DAG", e);
            return OBJECT_MAPPER.convertValue(dag, new TypeReference<Map<String, Object>>() {});
        }
    }
}
