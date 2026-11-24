package com.tapdata.tm.commons.dag.process.script;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import com.tapdata.tm.utils.JsFieldMapper;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author GavinXiao
 * @description ScriptProcessNode create by Gavin
 * @create 2023/6/19 18:13
 **/
@Getter
@Setter
@Slf4j
public class ScriptProcessNode extends ProcessorNode {
    @EqField
    protected String script;

    @EqField
    protected String declareScript;

    @EqField/* default value is 0, it's sepresents default JS ; if value is 1, */
    protected Integer jsType;

    /**
     * 当前推演服务无法试运行脚本（loadTapTable 返回 null，如引擎增量 DDL 推演使用的 DAGDataServiceImpl），仅保存在内存中
     */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private transient boolean trialRunUnavailable;

    /**
     * 本次推演待叠加的字段级 DDL 事件，由 {@link #fieldDdlEvent(TapDDLEvent)} 记录，仅保存在内存中
     */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private transient TapDDLEvent pendingFieldDdlEvent;

    /**
     * 本节点最近一次推演保存的输出模型，仅保存在内存中。引擎保存快照时会清空同一 DAG 各节点的 outputSchema，
     * 连续 DDL 推演需依赖此副本取得上一次 DDL 后的输出，而不是退回任务启动时的元数据模型
     */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private transient Schema lastSavedOutputSchema;

    protected TapTable getTapTable(Node target, TaskDto taskDtoCopy) {
        return service.loadTapTable(getId(), target.getId(), taskDtoCopy);
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        //py节点的模型可以是直接虚拟跑出来的。 跑出来就是正确的模型，由引擎负责传值给tm
        TapDDLEvent ddlEvent = pendingFieldDdlEvent;
        pendingFieldDdlEvent = null;
        if (schema != null) {
            return schema;
        }

        Schema inputSchema = SchemaUtils.mergeSchema(inputSchemas, null);
        if (trialRunUnavailable && null != ddlEvent && null != inputSchema) {
            // 无法试运行时不能用输入模型代替脚本输出，否则脚本新增/删除/转换的字段会丢失；基于上次输出模型叠加本次 DDL
            Schema merged = mergeWithPreviousOutput(inputSchema, ddlEvent);
            if (null != merged) {
                return merged;
            }
        }
        return inputSchema;
    }

    @Override
    public void fieldDdlEvent(TapDDLEvent event) throws Exception {
        if (ScriptDdlSchemaMerger.isFieldDdl(event)) {
            pendingFieldDdlEvent = event;
        }
    }

    @Override
    protected Schema saveSchema(Collection<String> predecessors, String nodeId, Schema schema, DAG.Options options) {
        Schema saved = super.saveSchema(predecessors, nodeId, schema, options);
        if (null != saved && CollectionUtils.isNotEmpty(saved.getFields())) {
            lastSavedOutputSchema = saved;
        }
        return saved;
    }

    protected Schema mergeWithPreviousOutput(Schema inputSchema, TapDDLEvent ddlEvent) {
        if (!ScriptDdlSchemaMerger.isDdlTable(inputSchema, ddlEvent)) {
            log.warn("Script node {}({}) can not match ddl table {}, use input schema", getId(), getName(), ddlEvent.getTableId());
            return null;
        }
        Schema previous = findPreviousOutput();
        if (null == previous) {
            return null;
        }
        Schema merged = ScriptDdlSchemaMerger.applyDdlOnPrevious(inputSchema, previous, ddlEvent);
        if (null == merged) {
            log.warn("Script node {}({}) can not apply ddl {} on previous output, use input schema",
                    getId(), getName(), ddlEvent.getClass().getSimpleName());
        }
        return merged;
    }

    protected Schema findPreviousOutput() {
        Schema lastOutput = getOutputSchema();
        if (null != lastOutput && CollectionUtils.isNotEmpty(lastOutput.getFields())) {
            return lastOutput;
        }
        if (null != lastSavedOutputSchema && CollectionUtils.isNotEmpty(lastSavedOutputSchema.getFields())) {
            return lastSavedOutputSchema;
        }
        if (service instanceof DAGDataServiceImpl) {
            String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), getId(), null);
            MetadataInstancesDto metadata = ((DAGDataServiceImpl) service).getMetadata(qualifiedName);
            if (null != metadata && CollectionUtils.isNotEmpty(metadata.getFields())) {
                return JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(metadata), Schema.class);
            }
        }
        return null;
    }

    @Override
    protected Schema loadSchema(DAG.Options options) {
        final List<String> predIds = new ArrayList<>();
        getPrePre(this, predIds);
        predIds.add(this.getId());
        Dag dag = this.getDag().toDag();
        List<Node> oldNodes = dag.getNodes();
        dag = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dag), Dag.class);
        List<Node> nodes = dag.getNodes();

        Node target = new VirtualTargetNode();
        target.setId(UUID.randomUUID().toString());
        target.setName(target.getId());
        if (CollectionUtils.isNotEmpty(nodes)) {
            for (Node node : nodes) {
                Optional<Node> optionalNode = oldNodes.stream().filter(o -> o.getId().equals(node.getId())).findFirst();
                if (optionalNode.isPresent()) {
                    node.setSchema(optionalNode.get().getSchema());
                    node.setOutputSchema(optionalNode.get().getOutputSchema());
                }
            }
            nodes = nodes.stream().filter(n -> predIds.contains(n.getId())).collect(Collectors.toList());
            nodes.add(target);
        }

        List<Edge> edges = dag.getEdges();
        if (CollectionUtils.isNotEmpty(edges)) {
            edges = edges.stream().filter(e -> (predIds.contains(e.getTarget()) || predIds.contains(e.getSource())) && !e.getSource().equals(this.getId())).collect(Collectors.toList());
            Edge edge = new Edge(this.getId(), target.getId());
            edges.add(edge);
        }


        dag.setNodes(nodes);
        dag.setEdges(edges);

        DAG build = DAG.build(dag);

        ObjectId taskId = this.getDag().getTaskId();
        TaskDto taskDto = service.getTaskById(taskId == null ? null : taskId.toHexString());
        TaskDto taskDtoCopy = new TaskDto();
        BeanUtils.copyProperties(taskDto, taskDtoCopy);
        taskDtoCopy.setStatus(TaskDto.STATUS_WAIT_RUN);
        taskDtoCopy.setSyncStatus("");
        taskDtoCopy.setSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);
        taskDtoCopy.setDag(build);
        taskDtoCopy.setId(Optional.of(new ObjectId(taskDto.getTransformTaskId())).orElseGet(ObjectId::new));
        taskDtoCopy.setName(taskDto.getName() + "(100)");
        List<Schema> inputSchema = getInputSchema();
        if (CollectionUtils.isEmpty(inputSchema)) {
            return null;
        }
        // Used to test-run data to get the model
        TapTable tapTable = null;
        if (!options.isPreview()) {
            tapTable = getTapTable(target, taskDtoCopy);
            trialRunUnavailable = null == tapTable;
        }

        if (tapTable == null) {
            return null;
        }

        String expression = null;
        label:
        for (Schema schema : inputSchema) {
            List<Field> fields = schema.getFields();
            if (CollectionUtils.isNotEmpty(fields)) {
                for (Field field : fields) {
                    String sourceDbType = field.getSourceDbType();
                    DataSourceDefinitionDto definition = ((DAGDataServiceImpl) service).getDefinitionByType(sourceDbType);
                    expression = definition.getExpression();
                    break label;
                }

            }
        }

        if (StringUtils.isNotBlank(expression)) {
            PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(tapTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(expression));
        }
        Schema schema = PdkSchemaConvert.fromPdkSchema(tapTable);

        Schema schema1 = inputSchema.get(0);
        if (schema1 != null) {
            schema.setDatabaseId(schema1.getDatabaseId());
            schema.setDatabase(schema1.getDatabase());
            schema.setName(schema1.getName());
            schema.setOriginalName(schema1.getOriginalName());
            schema.setCreateSource(schema1.getCreateSource());


            List<Field> fields1 = schema1.getFields();
            Map<String, Field> originFieldMap = fields1.stream().collect(Collectors.toMap(Field::getFieldName, f -> f));

            String sourceDbType = null;
            if (CollectionUtils.isNotEmpty(fields1)) {
                sourceDbType = fields1.get(0).getSourceDbType();
            }


            List<Field> fields = schema.getFields();

            if (CollectionUtils.isNotEmpty(fields)) {
                for (Field field : fields) {
                    field.setDataTypeTemp(field.getDataType());
                    field.setOriginalDataType(field.getDataType());
                    Field field1 = originFieldMap.get(field.getFieldName());
                    if (field1 != null) {
                        field1.setDataType(field.getDataType());
                        field1.setColumnPosition(field.getColumnPosition());
                        field1.setTapType(field.getTapType());
                        field1.setPrimaryKey(field.getPrimaryKey());
                        field1.setPrimaryKeyPosition(field.getPrimaryKeyPosition());
                        BeanUtils.copyProperties(field1, field);
                    } else {
                        field.setSourceDbType(sourceDbType);
                    }
                }
            }
        }
        analyseFields(schema, tapTable);
        return schema;
    }

    protected void analyseFields(Schema schema, TapTable sourceTable) {
        LinkedHashMap<String, TapField> sourceFieldMap = sourceTable.getNameFieldMap();
        Map<String, String> fieldNameMapping = JsFieldMapper.parseMapping(script);
        schema.getFields().stream()
                .filter(f -> fieldNameMapping.containsKey(f.getFieldName()))
                .forEach(field -> {
                    String fieldOriginName = fieldNameMapping.get(field.getFieldName());
                    TapField sourceField = sourceFieldMap.get(fieldOriginName);
                    Set<String> visitedFieldNames = new HashSet<>();
                    while (null == sourceField
                            && fieldNameMapping.containsKey(fieldOriginName)
                            && visitedFieldNames.add(fieldOriginName)) {
                        fieldOriginName = fieldNameMapping.get(fieldOriginName);
                        sourceField = sourceFieldMap.get(fieldOriginName);
                    }
                    if (sourceField == null) {
                        field.setOriginalFieldName(fieldOriginName);
                        field.setOriginalDataType(field.getDataType());
                        field.setOriginalScale(field.getScale());
                        field.setOriginalPrecision(field.getPrecision());
                        field.setOriginalJavaType(field.getJavaType());
                        field.setOriginalDefaultValue(field.getDefaultValue());

                        field.setPreviousFieldName(fieldOriginName);
                        field.setPreviousDataType(field.getDataType());
                    }
                });
    }

    /**
     * 创建处理器节点
     *
     * @param type 节点类型
     **/
    public ScriptProcessNode(String type) {
        super(type);
    }

    protected void getPrePre(Node node, List<String> preIds) {
        List<Node> predecessors = node.predecessors();
        if (CollectionUtils.isNotEmpty(predecessors)) {
            for (Node<?> predecessor : predecessors) {
                preIds.add(predecessor.getId());
                getPrePre(predecessor, preIds);
            }
        }
    }
}
