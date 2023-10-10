package com.tapdata.tm.commons.dag.process;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author GavinXiao
 * @description UnwindProcessNode create by Gavin
 * @create 2023/10/8 18:01
 * @doc https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
* */
@NodeType("unwind_processor")
@Slf4j
public class UnwindProcessNode extends ProcessorNode {

    private String nodeName;

    /** Field path to an array field.
     * To specify a field path,
     * prefix the field name and enclose in quotes. */
    private String path;

    /** Optional.
     * The name of a new field to hold the array index of the element.
     * The name cannot start with a dollar sign $. */
    private String includeArrayIndex;

    /** Optional.
     If true, if the path is null, missing, or an empty array,
     $unwind
     outputs the document.
     If false, if path is null, missing, or an empty array,
     $unwind
     does not output a document.
     The default value is false. */
    private boolean preserveNullAndEmptyArrays;

    public UnwindProcessNode() {
        super("unwind_processor");
    }

    @Override
    protected Schema loadSchema(List<String> includes) {
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
        taskDtoCopy.setSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);
        taskDtoCopy.setDag(build);
        taskDtoCopy.setId(Optional.of(new ObjectId(taskDto.getTransformTaskId())).orElseGet(ObjectId::new));
        taskDtoCopy.setName(taskDto.getName() + "(100)");
        List<Schema> inputSchema = getInputSchema();
        if (CollectionUtils.isEmpty(inputSchema)) {
            return null;
        }
        ////用于预跑数据得到模型
        TapTable tapTable = getTapTable(target, taskDtoCopy);

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

            if (originFieldMap.containsKey(path)) {
                Field field = originFieldMap.get(path);
                field.setTapType("TapMapping");
            }

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

        return schema;
    }

    protected TapTable getTapTable(Node target, TaskDto taskDtoCopy) {
        return service.loadTapTable(getId(), target.getId(), taskDtoCopy);
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

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getIncludeArrayIndex() {
        return includeArrayIndex;
    }

    public void setIncludeArrayIndex(String includeArrayIndex) {
        this.includeArrayIndex = includeArrayIndex;
    }

    public boolean isPreserveNullAndEmptyArrays() {
        return preserveNullAndEmptyArrays;
    }

    public void setPreserveNullAndEmptyArrays(boolean preserveNullAndEmptyArrays) {
        this.preserveNullAndEmptyArrays = preserveNullAndEmptyArrays;
    }
}
