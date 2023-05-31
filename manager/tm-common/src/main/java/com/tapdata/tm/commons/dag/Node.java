package com.tapdata.tm.commons.dag;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.exception.DDLException;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleVO;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/3 下午3:07
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Slf4j
@Getter
@Setter
public abstract class Node<S> extends Element{

    /**
     * 节点类型
     */
    private String type;

    /**
     * 节点类别：data-数据节点，processor-处理器节点
     */
    private NodeCatalog catalog;

    /**
     * 节点的校验错误消息
     */
    private List<Message> messages;

    /**
     * 当前节点的原始模型，每次推演时，都会在此基础上合并输入模型、应用字段处理器
     */
    @JsonIgnore
    private transient S schema;

    /**
     * 当前节点输出的模型，每次推演结束后生成的模型，会直接存储到模型库，可以依据类型映射进一步处理得到目标库创建数据结构的语句
     */
    @JsonIgnore
    private transient S outputSchema;


    //本次已经推演过
    private boolean isTransformed;

    private List<AlarmSettingVO> alarmSettings;
    private List<AlarmRuleVO> alarmRules;

    protected transient DAGDataService service;

    protected transient EventListener<S> listener;

    protected String externalStorageId;

//    private String subTaskId;

    /**
     * constructor for node
     * @param type 节点类型
     */
    public Node(String type) {
        super(ElementType.Node);
        this.type = type;
        this.catalog = NodeCatalog.data;
    }

    /**
     * constructor for node
     * @param type 节点类型
     * @param catalog 节点类别，默认为数据节点
     */
    public Node(String type, NodeCatalog catalog) {
        super(ElementType.Node);
        this.type = type;
        this.catalog = catalog;
    }

    public boolean isDataNode() {
        return NodeCatalog.data == catalog;
    }

    public boolean isLogCollectorNode() {
        return NodeCatalog.logCollector == catalog;
    }

    /**
     * 查询当前节点所有继任节点
     * @return
     */
    public List<Node<S>> successors() {
        Graph<? extends Element, ? extends Element> graph = getGraph();
        return graph.successors(getId()).stream().map(id -> (Node<S>)graph.getNode(id)).collect(Collectors.toList());
    }

    /**
     * 查询当前节点所有前任节点
     * @return
     */
    public List<Node<S>> predecessors() {
        Graph<? extends Element, ? extends Element> graph = getGraph();
        return graph.predecessors(getId()).stream().map(id -> (Node<S>)graph.getNode(id)).collect(Collectors.toList());
    }

    public LinkedList<Node<?>> getPreNodes(String nodeId) {
        return getDag().nodeMap().get(nodeId);
    }

    public List<DatabaseNode> getSourceNode() {
        return getDag().getSourceNode();
    }

    /**
     * 模型推演方法，当前节点的模型会重新计算，并且自动触发后面节点的 transformSchema
     */
    public void transformSchema() {
        transformSchema(null);
    }
    public void transformSchema(DAG.Options options) {
        //优化模型推演的顺序
        List<Node<S>> predNodes = predecessors();
        if (CollectionUtils.isNotEmpty(predNodes)) {
            for (Node<S> predNode : predNodes) {
                if (!predNode.isTransformed) {
                    predNode.transformSchema(options);
                    return;
                }
            }
        }

        if (StringUtils.equals("sync", options.getSyncType()) && isTransformed) {
            next(options);
            return;
        }

        String nodeId = getId();
        log.info("Transform schema for node {}({}), type = {}", nodeId, getName(), getType());

        boolean result = this.validate();
        if (!result) {
            log.error("Invalid node configuration, cancel transform schema: {}", messages);
            return;
        }

        if (schema == null) {
            try {
                schema = loadSchema(options.getIncludes());
                log.info("load schema complete");
            } catch (Exception e) {
                log.error("Load schema failed.", e);
            }
        }

        List<S> inputSchemas = getInputSchema();
        log.info("input schema = {}", inputSchemas == null ? null: inputSchemas.size());
        // 防止子类直接修改原始模型，这里需要对输入模型（inputSchema）、当前节点原始模型（schema）进行复制
        boolean mergedSchema = false;   // 输入模型为null，不进行merge操作，不需要执行保存更新
        if (inputSchemas != null && inputSchemas.size() > 0) {
            inputSchemas = inputSchemas.stream().map(this::cloneSchema).collect(Collectors.toList());

            if ("all".equals(options.getRollback())) {
                if (schema instanceof List) {
                    List<Schema> schemas = (List<Schema>) schema;
                    for (Schema schema1 : schemas) {
                        List<Field> deleteF = new ArrayList<>();
                        for (Field field : schema1.getFields()) {
                            if (field.isDeleted()) {
                                deleteF.add(field);
                            }
                        }
                        schema1.getFields().removeAll(deleteF);
                    }
                } else if (schema instanceof Schema) {
                    Schema schema1 = (Schema) schema;
                    List<Field> deleteF = new ArrayList<>();
                    for (Field field : schema1.getFields()) {
                        if (field.isDeleted()) {
                            deleteF.add(field);
                        }
                    }
                    schema1.getFields().removeAll(deleteF);
                }
            }
            outputSchema = mergeSchema(inputSchemas, cloneSchema(schema), options);
            log.info("merge schema complete");
            mergedSchema = true;  // 进行merge操作，需要执行保存/更新
        } else {
            this.outputSchema = cloneSchema(schema);
        }

        if (this.outputSchema != null) {
            S changedSchema = outputSchema;//filterChangedSchema(this.outputSchema, options);  // 过滤出修改过的模型
            if (changedSchema != null) {
                String taskId = service.getTaskId().toHexString();
                String version = options.getUuid();
                try {
                    Collection<String> predecessors = getGraph().predecessors(nodeId);
                    //需要保存的地方就可以存储异步推演的内容
                    log.info("save transform schema");
                    outputSchema = saveSchema(predecessors, nodeId, changedSchema, options);
                    List<String> sourceQualifiedNames = new ArrayList<>();
                    if (outputSchema instanceof List) {
                        List<Schema> outTemp = (List<Schema>) outputSchema;
                        if (CollectionUtils.isNotEmpty(outTemp)) {
                            sourceQualifiedNames = outTemp.stream().map(Schema::getQualifiedName).collect(Collectors.toList());
                        }
                    } else {
                        Schema outputSchema = (Schema) this.outputSchema;
                        sourceQualifiedNames = new ArrayList<>();
                        if (outputSchema != null) {
                            sourceQualifiedNames.add(outputSchema.getQualifiedName());
                        }
                    }
                    service.upsertTransformTemp(this.listener.getSchemaTransformResult(nodeId), taskId, nodeId, tableSize(), sourceQualifiedNames, version);
                } catch (Exception e) {
                    log.error("Save schema failed.", e);
                }
            } else {
                log.info("Schema is nothing changed for node {}.", nodeId);
            }

            if (listener != null){
                try {
                    listener.onTransfer(inputSchemas, schema, outputSchema, nodeId);
                } catch (Exception e) {
                    log.error("Call transfer listener failed in node {}", nodeId, e);
                }
            }
        } else {
            Collection<String> predecessors = getGraph().predecessors(nodeId);
            S changedSchema = outputSchema;//filterChangedSchema(this.outputSchema, options);
            if (schema instanceof Schema) {
                if (((Schema) schema).getSourceType().equals(SourceTypeEnum.SOURCE.name())) {
                    saveSchema(predecessors, nodeId, changedSchema, options);
                }
            } else if (schema instanceof List){
//                List<Schema> updateSchema = new ArrayList<>();
//                for (Schema o : ((List<Schema>) changedSchema)) {
//                    if ( o.getSourceType().equals(SourceTypeEnum.SOURCE.name())) {
//                        updateSchema.add(o);
//                    }
//                }
//
//                if (CollectionUtils.isNotEmpty(updateSchema)) {
//                    saveSchema(predecessors, nodeId, (S) updateSchema, options);
//                }

                saveSchema(predecessors, nodeId, changedSchema, options);
            }
        }


        // 触发后序节点模型推演
        next(options);

    }

    private void next(DAG.Options options) {
        Graph<? extends Element, ? extends Element> graph = getGraph();
        this.setTransformed(true);
        graph.successors(this.getId()).forEach(successorId -> {
            Element el = graph.getNode(successorId);
            if (el instanceof Node) {
                ((Node) el).transformSchema(options);
            }
        });
    }

    public abstract S mergeSchema(List<S> inputSchemas, S s, DAG.Options options);

    /**
     * 节点加载模型
     * @return
     */
    protected abstract S loadSchema(List<String> includes);

    /**
     * 保存模型，子类需要实现模型的存储
     * @param predecessors 前辈 id 列表
     * @param nodeId 当前节点 ID
     * @param schema 当前节点模型
     * @param options 推演模型的配置项
     */
    protected abstract S saveSchema(Collection<String> predecessors, String nodeId, S schema, DAG.Options options);

    /**
     * 比较 两个模型是否一样
     * @param s1
     * @param s2
     * @return
     */
    protected boolean compareSchemaEquals(S s1, S s2) {
        if (s1 == null && s2 == null)
            return true;
        if (s1 != null)
            return s1.equals(s2);
        else
            return false;
    }

    /**
     * 在保存模型之前调用这个方法过滤出变化的模型，返回结果 传给 {@see com.tapdata.tm.commons.dag.DAGDataService#createOrUpdateSchema} 方法保存
     * @param outputSchema 当前节点输出模型
     * @return 修改过的模型，返回 null 不执行保存
     */
    protected S filterChangedSchema(S outputSchema, DAG.Options options) {
        if (options != null && options.getCustomTypeMappings() != null && options.getCustomTypeMappings().size() > 0) {
            return outputSchema;
        }
        return compareSchemaEquals(this.schema, outputSchema) ? null : outputSchema;
    }


    public int tableSize() {
        return 1;
    }

    /**
     * 获取输入模型
     */
    public List<S> getInputSchema() {

        Graph<? extends Element, ? extends Element> graph = getGraph();
        return graph.predecessors(getId()).stream().map(predecessorId -> {
            Element predecessor = graph.getNode(predecessorId);
            if (predecessor instanceof Node) {
                return ((Node<S>)predecessor).getOutputSchema();
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    /**
     * 复制模型
     * @param s
     * @return
     */
    protected abstract S cloneSchema(S s) ;

    /**
     * 节点数据验证方法
     * @return 验证失败的说明
     */
    public boolean validate() {
        return true;
    }

    public SourceType sourceType() {
        boolean source = true;
        boolean target = true;
        Collection<String> predecessors = getGraph().predecessors(this.getId());
        Collection<String> successors = getGraph().successors(this.getId());
        if (predecessors.isEmpty()) {
            target = false;
        }

        if (successors.isEmpty()) {
            source = false;
        }

        if (source && target) {
            return SourceType.source_and_target;
        }

        if (source) {
            return SourceType.source;
        }

        return SourceType.target;
    }


    public enum SourceType {
        source,
        target,
        source_and_target,
        ;
    }

    /**
     * 初始化完成节点的属性信息后，自动执行这个方法
     */
    public void afterProperties(){}

    public enum NodeCatalog {
        // 数据节点
        data,
        // 处理器节点
        processor,
        logCollector,
        memCache,

        virtualTarget,
    }

    public interface EventListener<S> {
        /**
         * 节点从前任节点传递到当前节点时触发
         * @param inputSchemaList 前任节点模型列表
         * @param schema 当前节点原始模型
         * @param outputSchema 当前节点输出模型
         * @param nodeId 当前节点id
         */
        void onTransfer(List<S> inputSchemaList, S schema, S outputSchema, String nodeId);

        /**
         * 节点转换模型的结果
         * @param schemaTransformerResults
         * @param nodeId
         */
        void schemaTransformResult(String nodeId, Node node, List<SchemaTransformerResult> schemaTransformerResults);

        List<SchemaTransformerResult> getSchemaTransformResult(String nodeId);
    }

    public static boolean fieldEq(Object f1, Object f2) {
        if (f1 == null && f2 == null) {
            return true;
        }

        if (f1 == null || f2 == null) {
            return false;
        }

        return f1.equals(f2);
    }


    protected void fieldNameUpLow(List<String> inputFields, List<Field> fields, String fieldsNameTransform) {
        if (fieldsNameTransform != null) {
            if ("toUpperCase".equalsIgnoreCase(fieldsNameTransform)) {
                fields.forEach(field -> {
                    if (inputFields.contains(field.getOriginalFieldName()) && !field.isDeleted()) {
                        String fieldName = field.getFieldName();
                        fieldName = fieldName.toUpperCase();
                        field.setFieldName(fieldName);
                    }
                });
            } else if ("toLowerCase".equalsIgnoreCase(fieldsNameTransform)) {
                fields.forEach(field -> {
                    if (inputFields.contains(field.getOriginalFieldName()) && !field.isDeleted()) {
                        String fieldName = field.getFieldName();
                        fieldName = fieldName.toLowerCase();
                        field.setFieldName(fieldName);
                    }
                });
            }
        }

    }

    protected void fieldNameReduction(List<String> inputFields, List<Field> fields, String fieldsNameTransform) {
        //对于不变的这种类型的特殊处理
        if (fieldsNameTransform != null) {
            if ("".equals(fieldsNameTransform)) {
                fields.forEach(field -> {
                    if (inputFields.contains(field.getOriginalFieldName())) {
                        field.setFieldName(field.getOriginalFieldName());
                    }
                });
            }
        }
    }


    /**
     * 对于ddl时间的node节点改动处理， 默认不处理
     *  目前需要处理的有，表节点改名，删除，新增， 数据库节点，改名，删除，新增
     *  字段改名节点    字段新增节点    字段删除节点
     * @param event
     */
    public void fieldDdlEvent(TapDDLEvent event) throws Exception {

    }

    protected void updateDdlList(List<String> updateList, TapDDLEvent event) throws Exception {
        if (CollectionUtils.isEmpty(updateList)) {
            return;
        }
        if (event instanceof TapAlterFieldNameEvent) {
            ValueChange<String> nameChange = ((TapAlterFieldNameEvent) event).getNameChange();
            String changeField = null;
            for (String updateConditionField : updateList) {
                if (updateConditionField.equals(nameChange.getBefore())) {
                    changeField = updateConditionField;
                }
            }
            if (changeField != null) {
                updateList.remove(changeField);
                updateList.add(nameChange.getAfter());
            }

        } else if (event instanceof TapDropFieldEvent) {
            String fieldName = ((TapDropFieldEvent) event).getFieldName();
            if (updateList.contains(fieldName)) {
                throw new DDLException("Ddl drop field link update condition fields");
            }
        }
    }

    public String getTaskId() {
        DAG dag = getDag();
        if (dag != null && dag.getTaskId() != null) {
            return dag.getTaskId().toHexString();
        }
        return null;
    }
}
