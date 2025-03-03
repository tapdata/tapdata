package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 华为 DRS Kafka 消息转换器 - 模型推演
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/18 15:27 Create
 */
@Setter
@Getter
@Slf4j
@NodeType(HuaweiDrsKafkaConvertorNode.TYPE)
public class HuaweiDrsKafkaConvertorNode extends ProcessorNode {
    public static final String TYPE = "huawei_drs_kafka_convertor";

    private String storeType;
    private String fromDBType;
    private int sampleSize = 10;

    public HuaweiDrsKafkaConvertorNode() {
        super(TYPE);
    }

    @Override
    protected Schema loadSchema(DAG.Options options) {
        if (TaskDto.SYNC_TYPE_PREVIEW.equals(options.getSyncType())) return null;

        List<Schema> inputSchemas = getInputSchema();
        if (CollectionUtils.isEmpty(inputSchemas)) return null;

        // 生成 sample 任务
        String sampleNodeId = UUID.randomUUID().toString();
        TaskDto sampleTask = buildSampleTask(sampleNodeId);
        if (null != sampleTask) {
            // 根据 sample 任务跑出模型
            TapTable sampleTable = getService().loadTapTable(getId(), sampleNodeId, sampleTask);

            if (null != sampleTable && null != sampleTable.getNameFieldMap()) {
                // 根据源节点处理字段类型
                resetFieldTypeWithFirstExpression(inputSchemas, sampleTable);

                // 将 TapTable 转成 Schema
                return sampleTapTable2Schema(inputSchemas, sampleTable);
            }
        }
        return null;
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        // schema 由 FE 运行 sample 任务所得，可以直接返回
        return Optional.ofNullable(schema)
            .orElseGet(() -> super.mergeSchema(inputSchemas, null, options));
    }

    // ---------- 内部函数 ----------

    protected TaskDto buildSampleTask(String sampleNodeId) {
        return Optional.ofNullable(getDag()).map(DAG::getTaskId)
            .map(taskId -> getService().getTaskById(taskId.toHexString()))
            .map(taskDto -> {
                DAG sampleDag = buildSampleDag(sampleNodeId);
                if (null != sampleDag) {
                    TaskDto sampleTask = new TaskDto();
                    BeanUtils.copyProperties(taskDto, sampleTask);
                    sampleTask.setStatus(TaskDto.STATUS_WAIT_RUN);
                    sampleTask.setSyncStatus("");
                    sampleTask.setSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);
                    sampleTask.setDag(sampleDag);
                    sampleTask.setId(Optional.of(new ObjectId(taskDto.getTransformTaskId())).orElseGet(ObjectId::new));
                    sampleTask.setName(taskDto.getName() + "(100)");
                    return sampleTask;
                }
                return null;
            })
            .orElse(null);
    }

    protected DAG buildSampleDag(String sampleNodeId) {
        // 没有前置连线，无法加载模型
        List<String> currentAndPredIds = getPredecessorsIds();
        if (currentAndPredIds.isEmpty()) return null;

        currentAndPredIds.add(this.getId());

        Dag oldDag = this.getDag().toDag();
        List<Node> oldNodes = oldDag.getNodes();
        // 使 sample 节点实例与当前实例脱离
        Dag newDag = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(oldDag), Dag.class);
        if (null == newDag) {
            log.warn("can't create sample dag with taskId(" + getTaskId() + ") nodeId(" + getId() + ")", getTaskId(), getId());
            return null;
        }

        newDag.setNodes(createNewNodes(newDag, oldNodes, currentAndPredIds));
        newDag.setEdges(createNewEdges(newDag, currentAndPredIds));

        // 添加虚拟节点到任务 DAG
        VirtualTargetNode targetNode = new VirtualTargetNode();
        targetNode.setId(sampleNodeId);
        targetNode.setName(targetNode.getId());
        newDag.getNodes().add(targetNode);
        newDag.getEdges().add(new Edge(this.getId(), targetNode.getId()));

        return DAG.build(newDag);
    }

    protected void resetFieldTypeWithFirstExpression(List<Schema> inputSchemas, TapTable sampleTapTable) {
        for (Schema schema : inputSchemas) {
            if (null == schema.getFields()) continue;
            for (Field field : schema.getFields()) {
                String sourceDbType = field.getSourceDbType();
                if (Optional.ofNullable(((DAGDataServiceImpl) getService()).getDefinitionByType(sourceDbType))
                    .map(DataSourceDefinitionDto::getExpression)
                    .map(expression -> {
                        PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(sampleTapTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(expression));
                        return true;
                    }).orElse(false)) return;
            }
        }
    }

    protected Schema sampleTapTable2Schema(List<Schema> inputSchemas, TapTable sampleTapTable) {
        Schema schema = PdkSchemaConvert.fromPdkSchema(sampleTapTable);
        Schema inputSchema = inputSchemas.get(0);
        if (null == inputSchema) return schema;

        String sourceDbType = null;
        List<Field> originFieldList = inputSchema.getFields();
        Map<String, Field> originFieldMap = originFieldList.stream().collect(Collectors.toMap(Field::getFieldName, f -> f));
        if (CollectionUtils.isNotEmpty(originFieldList)) {
            sourceDbType = originFieldList.get(0).getSourceDbType();
        }

        schema.setDatabaseId(inputSchema.getDatabaseId());
        schema.setDatabase(inputSchema.getDatabase());
        schema.setName(inputSchema.getName());
        schema.setOriginalName(inputSchema.getOriginalName());
        schema.setCreateSource(inputSchema.getCreateSource());

        List<Field> fields = schema.getFields();
        if (CollectionUtils.isNotEmpty(fields)) {
            for (Field field : fields) {
                field.setDataTypeTemp(field.getDataType());
                field.setOriginalDataType(field.getDataType());
                Field newField = originFieldMap.get(field.getFieldName());
                if (newField == null) {
                    field.setSourceDbType(sourceDbType);
                } else {
                    newField.setDataType(field.getDataType());
                    newField.setColumnPosition(field.getColumnPosition());
                    newField.setTapType(field.getTapType());
                    newField.setPrimaryKey(field.getPrimaryKey());
                    newField.setPrimaryKeyPosition(field.getPrimaryKeyPosition());
                    BeanUtils.copyProperties(newField, field);
                }
            }
        }
        return schema;
    }

    protected List<String> getPredecessorsIds() {
        List<String> preIds = new ArrayList<>();
        addPredecessorsId2List(this.predecessors(), preIds);
        return preIds;
    }

    protected void addPredecessorsId2List(List<Node<Schema>> predecessors, List<String> preIds) {
        if (null == predecessors) return;
        for (Node<Schema> node : predecessors) {
            preIds.add(node.getId());
            addPredecessorsId2List(node.predecessors(), preIds);
        }
    }

    protected List<Node> createNewNodes(Dag newDag, List<Node> oldNodes, List<String> currentAndPredIds) {
        List<Node> newNodes = new ArrayList<>();
        for (Node newNode : newDag.getNodes()) {
            // 只取节点前的配置
            if (!currentAndPredIds.contains(newNode.getId())) continue;

            // 设置源抽样数据量
            if (newNode instanceof TableNode) {
                ((TableNode) newNode).setRows(getSampleSize());
            }

            // 将 schema 相关属性设置到新节点上
            setSchemaProps(oldNodes, newNode);
            newNodes.add(newNode);
        }
        return newNodes;
    }

    protected List<Edge> createNewEdges(Dag newDag, List<String> currentAndPredIds) {
        List<Edge> newEdges = new ArrayList<>();
        for (Edge e : newDag.getEdges()) {
            if (e.getSource().equals(this.getId())) continue;
            if (currentAndPredIds.contains(e.getTarget()) || currentAndPredIds.contains(e.getSource())) {
                newEdges.add(e);
            }
        }
        return newEdges;
    }

    protected void setSchemaProps(List<Node> nodeList, Node<Schema> newNode) {
        for (Node<Schema> node : nodeList) {
            if (newNode.getId().equals(node.getId())) {
                newNode.setSchema(node.getSchema());
                newNode.setOutputSchema(node.getOutputSchema());
                break;
            }
        }
    }
}
