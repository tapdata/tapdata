package com.tapdata.tm.group.vo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
public class ResourceDiffItem {
    private String name;
    /** for tasks: migrate / sync / validate / shareCache; null for others */
    private String type;
    /** 字段级变更列表，add 项为 null */
    private List<FieldChange> changes;
    /** DAG 变更按类型分类（节点增删、配置修改、连线增删） */
    private DagChangeDetail dagChangeDetail;
    /** configPath -> display label from spec.json, e.g. "config.database" -> "Database Name" */
    private Map<String, String> fieldLabels;
    /** 任务同步模式: initial_sync+cdc / initial_sync / cdc */
    private String syncType;
    /** 源表→目标表映射，key=源表名, value=目标表名 */
    private LinkedHashMap<String, String> tableMapping;
    /** 连接-数据库类型，如 mysql, mongodb */
    private String databaseType;
    /** 连接-连接类型：source_and_target / source / target */
    private String connectionType;
    /** API-访问路径 */
    private String apiPath;
    /** API-数据库连接名 */
    private String apiConnectionName;
    /** API-表名 */
    private String apiTableName;

    public ResourceDiffItem(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public ResourceDiffItem(String name, String type, List<FieldChange> changes) {
        this.name = name;
        this.type = type;
        this.changes = changes;
    }
}