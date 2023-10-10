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
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Long;

/**
 * @author GavinXiao
 * @description UnwindProcessNode create by Gavin
 * @create 2023/10/8 18:01
 * @doc https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
* */
@NodeType("unwind_processor")
@Slf4j
public class UnwindProcessNode extends ProcessorNode {
    public static final String SUF_PATH_KEY = "$";

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

        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (nameFieldMap.containsKey(path)) {
            TapField field = nameFieldMap.get(path);
            field.setTapType(new TapMap());
            if (null != includeArrayIndex && !"".equals(includeArrayIndex.trim())) {
                TapField includeArrayIndexField = field(includeArrayIndex, JAVA_Long);
                includeArrayIndexField.setName(includeArrayIndex);
                includeArrayIndexField.setComment("The name of a new field to hold the array index of the element. The name cannot start with a dollar sign $.");
                tapTable.add(includeArrayIndexField);
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
        if (null != path && path.startsWith(SUF_PATH_KEY) && !SUF_PATH_KEY.equals(path.trim())) return path.substring(1);
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
