package com.tapdata.tm.commons.dag.process;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("js_processor")
@Getter
@Setter
public class JsProcessorNode extends ProcessorNode {
    @EqField
    private String script;

    @EqField
    private String declareScript;


    @Override
    protected Schema loadSchema(List<String> includes) {
        List<Node<Schema>> predecessors = this.predecessors();
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
        taskDtoCopy.setId(new ObjectId());
        taskDtoCopy.setName(taskDto.getName() + "(100)");
        List<Schema> inputSchema = getInputSchema();
        if (CollectionUtils.isEmpty(inputSchema)) {
            return null;
        }
        ////用于预跑数据得到模型
        TapTable tapTable = service.loadTapTable(getInputSchema(), script, getId(), target.getId(), null, null, taskDtoCopy);

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
            PdkSchemaConvert.tableFieldTypesGenerator.autoFill(tapTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(expression));
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

            Set<String> fieldNames = fields.stream().map(Field::getFieldName).collect(Collectors.toSet());

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

            List<TableIndex> indices = new ArrayList<>();

            if (CollectionUtils.isNotEmpty(schema1.getIndices())) {
                for (TableIndex index : schema1.getIndices()) {
                    List<TableIndexColumn> columns = index.getColumns();
                    List<TableIndexColumn> newColumns = new ArrayList<>();
                    for (TableIndexColumn column : columns) {
                        if (fieldNames.contains(column.getColumnName())){
                            newColumns.add(column);
                        }
                    }
                    if (CollectionUtils.isNotEmpty(newColumns)) {
                        index.setColumns(newColumns);
                        indices.add(index);
                    }
                }
            }

            schema.setIndices(indices);
        }

        return schema;
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        //js节点的模型可以是直接虚拟跑出来的。 跑出来就是正确的模型，由引擎负责传值给tm
        if (schema != null) {
            return schema;
        }

        return SchemaUtils.mergeSchema(inputSchemas, null);
    }

    public JsProcessorNode() {
        super("js_processor");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof JsProcessorNode) {
            Class className = JsProcessorNode.class;
            for (; className != Object.class; className = className.getSuperclass()) {
                java.lang.reflect.Field[] declaredFields = className.getDeclaredFields();
                for (java.lang.reflect.Field declaredField : declaredFields) {
                    EqField annotation = declaredField.getAnnotation(EqField.class);
                    if (annotation != null) {
                        try {
                            Object f2 = declaredField.get(o);
                            Object f1 = declaredField.get(this);
                            boolean b = fieldEq(f1, f2);
                            if (!b) {
                                return false;
                            }
                        } catch (IllegalAccessException e) {
                        }
                    }
                }
            }
            return true;
        }
        return false;
    }

    private void getPrePre(Node node, List<String> preIds) {
        List<Node> predecessors = node.predecessors();
        if (CollectionUtils.isNotEmpty(predecessors)) {
            for (Node predecessor : predecessors) {
                preIds.add(predecessor.getId());
                getPrePre(predecessor, preIds);
            }
        }
    }
}
