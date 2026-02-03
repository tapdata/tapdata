package com.tapdata.tm.task.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.tm.commons.task.dto.TaskDto;
import lombok.extern.slf4j.Slf4j;

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

            // 告警与通知 (Alerting & Notification)
            "alarmRules",                   // 告警规则
            "alarmSettings",                // 告警设置
            "emailReceivers",               // 邮件接收人列表
            "notifyTypes",                  // 通知设置

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
        config.put("dag", task.getDag());
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
}

