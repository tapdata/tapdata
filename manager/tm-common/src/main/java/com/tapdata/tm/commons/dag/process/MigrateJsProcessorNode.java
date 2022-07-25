package com.tapdata.tm.commons.dag.process;

import com.google.common.collect.Lists;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.vo.MigrateJsResultVo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;

import java.time.OffsetDateTime;
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
        subTaskDto.setParentSyncType(taskDto.getSyncType());

        List<MigrateJsResultVo> jsResult = service.getJsResult(getId(), target.getId(), subTaskDto);
        if (CollectionUtils.isEmpty(jsResult)) {
            throw new RuntimeException("migrate js result is null");
        }

        Map<String, MigrateJsResultVo> removeMap = jsResult.stream()
                .filter(n -> "REMOVE".equals(n.getOp()) || "CONVERT".equals(n.getOp()))
                .collect(Collectors.toMap(MigrateJsResultVo::getFieldName, Function.identity(), (e1, e2) -> e1));

        Map<String, MigrateJsResultVo> convertMap = jsResult.stream()
                .filter(n -> "CONVERT".equals(n.getOp()))
                .collect(Collectors.toMap(MigrateJsResultVo::getFieldName, Function.identity(), (e1, e2) -> e1));

        Map<String, TapField> createMap = jsResult.stream()
                .filter(n -> "CREATE".equals(n.getOp()))
                .collect(Collectors.toMap(MigrateJsResultVo::getFieldName, MigrateJsResultVo::getTapField, (e1, e2) -> e1));

        List<Schema> inputSchema = getInputSchema().get(0);
        if (CollectionUtils.isEmpty(inputSchema)) {
            return null;
        }

        // js result data
        List<Schema> result = Lists.newArrayList();
        for (Schema schema : inputSchema) {

            TapTable tapTable = PdkSchemaConvert.toPdk(schema);
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();

            if (!removeMap.isEmpty()) {
                removeMap.keySet().forEach(nameFieldMap::remove);
            }

            if (!convertMap.isEmpty()) {
                convertMap.keySet().forEach(name -> {
                    if (nameFieldMap.containsKey(name)) {
                        nameFieldMap.put(name, convertMap.get(name).getTapField());
                    }
                });
            }

            if (!createMap.isEmpty()) {
                nameFieldMap.putAll(createMap);
            }

            result.add(PdkSchemaConvert.fromPdkSchema(tapTable));
        }

        return result;
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
