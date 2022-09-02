package com.tapdata.tm.task.constant;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.EnumSet;
import java.util.LinkedList;

@AllArgsConstructor
@Getter
public enum DagOutputTemplateEnum {
    AGENT_CAN_USE_CHECK("Agent可用性检测", "agentStrategy", DagOutputTemplate.AGENT_CAN_USE_INFO, DagOutputTemplate.AGENT_CAN_USE_ERROR),
    TASK_SETTING_CHECK("任务设置检测", "taskSettingStrategy", DagOutputTemplate.TASK_SETTING_INFO, DagOutputTemplate.TASK_SETTING_ERROR),
    SOURCE_SETTING_CHECK("源节点设置检测", "sourceSettingStrategy", DagOutputTemplate.SOURCE_SETTING_INFO, DagOutputTemplate.SOURCE_SETTING_ERROR),
    JS_NODE_CHECK("JS节点设置检测", "jsSettingStrategy", DagOutputTemplate.JS_NODE_INFO, DagOutputTemplate.JS_NODE_ERROR),
    TABLE_EDIT_NODE_CHECK("表编辑节点设置检测", "tableEditStrategy", DagOutputTemplate.TABLE_EDIT_NODE_INFO, DagOutputTemplate.TABLE_EDIT_NODE_ERROR),
    FIELD_EDIT_NODE_CHECK("字段编辑节点设置检测", "fieldEditStrategy", DagOutputTemplate.FIELD_EDIT_NODE_INFO, DagOutputTemplate.FIELD_EDIT_NODE_ERROR),
    TARGET_NODE_CHECK("目标节点设置检测", "targetSettingStrategy", DagOutputTemplate.TARGET_NODE_INFO, DagOutputTemplate.TARGET_NODE_ERROR),
    SOURCE_CONNECT_CHECK("源连接检测", "sourceConnectStrategy", DagOutputTemplate.SOURCE_CONNECT_INFO, DagOutputTemplate.SOURCE_CONNECT_ERROR),
    TARGET_CONNECT_CHECK("目标连接检测", "targetConnectStrategy", DagOutputTemplate.TARGET_CONNECT_INFO, DagOutputTemplate.TARGET_CONNECT_ERROR),
    CHARACTER_ENCODING_CHECK("字符编码检测", "characterStrategy", DagOutputTemplate.CHARACTER_ENCODING_INFO, DagOutputTemplate.CHARACTER_ENCODING_WARN),
    TABLE_NAME_CASE_CHECK("表名大小写检测", "tableCaseStrategy", DagOutputTemplate.TABLE_NAME_CASE_INFO, DagOutputTemplate.TABLE_NAME_CASE_ERROR),
    MODEL_PROCESS_CHECK("模型推演检测", "modelProcessStrategy", DagOutputTemplate.MODEL_PROCESS_INFO, DagOutputTemplate.MODEL_PROCESS_ERROR),
    DATA_INSPECT_CHECK("数据校验检测", "dataInspectStrategy", DagOutputTemplate.DATA_INSPECT_INFO, DagOutputTemplate.DATA_INSPECT_ERROR),
    ;

    private final String type;
    private final String beanName;
    private final String infoTemplate;
    private final String errorTemplate;
    

    public static LinkedList<DagOutputTemplateEnum> getSaveCheck() {
        EnumSet<DagOutputTemplateEnum> enumSet = EnumSet.of(TASK_SETTING_CHECK, SOURCE_SETTING_CHECK, JS_NODE_CHECK, TABLE_EDIT_NODE_CHECK, FIELD_EDIT_NODE_CHECK, TARGET_NODE_CHECK, DATA_INSPECT_CHECK, MODEL_PROCESS_CHECK);
        return Lists.newLinkedList(enumSet);
    }

    public static LinkedList<DagOutputTemplateEnum> getStartCheck() {
        EnumSet<DagOutputTemplateEnum> enumSet = EnumSet.of(AGENT_CAN_USE_CHECK, SOURCE_CONNECT_CHECK, TARGET_CONNECT_CHECK, CHARACTER_ENCODING_CHECK, TABLE_NAME_CASE_CHECK, MODEL_PROCESS_CHECK, DATA_INSPECT_CHECK);
        return Lists.newLinkedList(enumSet);
    }
}
