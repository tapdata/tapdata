package com.tapdata.tm.commons.dag.process;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.vo.MigrateJsResultVo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.schema.TapTable;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.tapdata.tm.commons.base.convert.ObjectIdDeserialize.toObjectId;

/**
 * @Author: Zed
 * @Date: 2021/11/5
 * @Description:
 */
@NodeType("migrate_js_processor")
@Getter
@Setter
public class MigrateJsProcessorNode extends Node<List<Schema>> {
    @EqField
    private String script;

    @Override
    protected List<Schema> loadSchema(List<String> includes) {
        String nodeId = this.getId();
        Dag dag = this.getDag().toDag();
        dag = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dag), Dag.class);
        List<Node<?>> nodes = this.getDag().nodeMap().get(nodeId);

        Node<?> target = new VirtualTargetNode();
        target.setId(UUID.randomUUID().toString());
        target.setName(target.getId());
        if (CollectionUtils.isNotEmpty(nodes)) {
            nodes.add(target);
        }

        List<Edge> edges = this.getDag().edgeMap().get(nodeId);
        if (CollectionUtils.isNotEmpty(edges)) {
            Edge edge = new Edge(nodeId, target.getId());
            edges.add(edge);
        }

        dag.setNodes(new LinkedList<Node>(){{addAll(nodes);}});
        dag.setEdges(edges);

        DAG build = DAG.build(dag);

        SubTaskDto subTaskDto = new SubTaskDto();
        subTaskDto.setStatus(SubTaskDto.STATUS_WAIT_RUN);
        ObjectId taskId = this.getDag().getTaskId();
        TaskDto taskDto = service.getTaskById(taskId == null ? null : taskId.toHexString());
        taskDto.setDag(null);
        subTaskDto.setParentTask(taskDto);
        subTaskDto.setDag(build);
        subTaskDto.setParentId(taskDto.getId());
        subTaskDto.setId(new ObjectId());
        subTaskDto.setName(taskDto.getName() + "(100)");
        subTaskDto.setTransformTask(true);
        ////用于预跑数据得到模型
        List<MigrateJsResultVo> jsResult = service.getJsResult(getId(), target.getId(), subTaskDto);
        if (CollectionUtils.isEmpty(jsResult)) {
            throw new RuntimeException("migrate js result is null");
        }

//        jsResult.stream().map(MigrateJsResultVo::getFieldName, Function.identity())

        Schema schema = null;

        List<List<Schema>> inputSchema = getInputSchema();
        if (CollectionUtils.isEmpty(inputSchema)) {
            return null;
        }


        Schema schema1 = null;
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

        return null;
    }

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas) {
        //js节点的模型可以是直接虚拟跑出来的。 跑出来就是正确的模型，由引擎负责传值给tm
        return schemas;
    }

    public MigrateJsProcessorNode() {
        super("migrate_js_processor");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof MigrateJsProcessorNode) {
            Class className = MigrateJsProcessorNode.class;
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

    @Override
    protected List<Schema> saveSchema(Collection<String> predecessors, String nodeId, List<Schema> schema, DAG.Options options) {
        ObjectId taskId = taskId();
        schema.forEach(s -> {
            s.setTaskId(taskId);
            s.setNodeId(nodeId);
        });

        return service.createOrUpdateSchema(ownerId(), toObjectId(getConnectId()), schema, options, this);
    }

    @Override
    protected List<Schema> cloneSchema(List<Schema> schemas) {
        if (schemas == null) {
            return Collections.emptyList();
        }
        return SchemaUtils.cloneSchema(schemas);
    }

    private String getConnectId() {
        AtomicReference<String> connectionId = new AtomicReference<>("");

        getSourceNode().stream().findFirst().ifPresent(node -> connectionId.set(node.getConnectionId()));
        return connectionId.get();
    }
}
