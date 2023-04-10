package com.tapdata.tm.commons.dag.logCollector;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;



/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/5 上午10:11
 * @description
 */
@NodeType("logCollector")
@Getter
@Setter
@ToString(callSuper = true)
@Slf4j
public class LogCollectorNode extends Node<List<Schema>> {

    /** 所有表 */
    public static final String SELECT_TYPE_ALL = "allTables";
    /** 保留表 */
    public static final String SELECT_TYPE_RESERVATION = "reservationTable";
    /** 排除表 */
    public static final String SELECT_TYPE_EXCLUSIONTABLE = "exclusionTable";

    @EqField
    /** 数据源id, 多个数据源本质是同一个数据源连接的时候，会存在多个数据源id */
    private List<String> connectionIds;
    /** 数据源类型 */
    private String databaseType;

    @EqField
    /** 表名 ， selectType为 allTables ：就不用填写表名， reservationTable： 为需要缓存的表， exclusionTable：不需要缓存的表名 */
    private List<String> tableNames;

    private Map<String, LogCollecotrConnConfig> logCollectorConnConfigs;

    @EqField
    /**
     *  allTables - 所有表
     *  reservationTable - 保留表
     *  exclusionTable - 排除表
     **/
    private String selectType = SELECT_TYPE_ALL;

    @EqField
    /** 保留的时长 单位 （天） 默认3天*/
    private Integer storageTime = 3;

    /** //  current - 从浏览器当前时间
     //  localTZ - 从指定的时间开始(浏览器时区)
     //  connTZ - 从指定的时间开始(数据库时区)*/
    @EqField
    @Deprecated
    private String syncTimePoint = "current";
    /** 时区 */
    @EqField
    @Deprecated
    private String syncTineZone;

    /** 时间 */
    @EqField
    @Deprecated
    private Date syncTime;



    public LogCollectorNode() {
        super("logCollector", NodeCatalog.logCollector);
    }

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas, DAG.Options options) {
        return schemas;
    }

    @Override
    protected List<Schema> loadSchema(List<String> includes) {
        return Collections.emptyList();
    }

    @Override
    protected List<Schema> saveSchema(Collection<String> pre, String nodeId, List<Schema> schemaList, DAG.Options options) {
        return null;
    }

    @Override
    protected List<Schema> cloneSchema(List<Schema> schemas) {
        if (schemas == null) {
            return Collections.emptyList();
        }
        return schemas.stream().map(SchemaUtils::cloneSchema).collect(Collectors.toList());
    }

    @Override
    protected List<Schema> filterChangedSchema(List<Schema> outputSchema, DAG.Options options) {

        if (outputSchema == null || outputSchema.size() == 0) {
            return Collections.emptyList();
        }
        List<Schema> originalSchemaList = getSchema() != null ? getSchema() : Collections.emptyList();
        Map<String, Schema> originalSchemaMap = originalSchemaList.stream().collect(Collectors.toMap(Schema::getOriginalName, s -> s, (s1, s2) -> s1));

        // 于原始模型列表比较，过滤掉没有变化过的模型
        return outputSchema.stream().filter(s -> {
            if (originalSchemaMap.containsKey(s.getOriginalName()) && s.equals(originalSchemaMap.get(s.getOriginalName()))) {
                return false;
            }
            return true;
        }).collect(Collectors.toList());
    }
}
