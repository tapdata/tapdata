package com.tapdata.tm.commons.dag.process;

import cn.hutool.core.bean.BeanUtil;
import com.google.common.collect.Lists;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.vo.MigrateJsResultVo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class MigrateJsProcessorNode extends MigrateProcessorNode {
    @EqField
    private String script;

    @EqField
    private String declareScript;

    @Override
    protected List<Schema> loadSchema(List<String> includes) {
        Map<String, List<Schema>> schemaMap = new HashMap<>();
        Map<String, List<Schema>> outSchemaMap = new HashMap<>();
        this.getDag().getNodes().forEach(n -> {
            schemaMap.put(n.getId(), (List<Schema>) n.getSchema());
            outSchemaMap.put(n.getId(), (List<Schema>) n.getOutputSchema());
        });

        final List<String> predIds = new ArrayList<>();
        getPrePre(this, predIds);
        predIds.add(this.getId());
        Dag dag = this.getDag().toDag();
        dag = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dag), Dag.class);
        List<Node> nodes = dag.getNodes();

        VirtualTargetNode target = new VirtualTargetNode();
        target.setId(UUID.randomUUID().toString());
        target.setName(target.getId());
        if (CollectionUtils.isNotEmpty(nodes)) {
            nodes = nodes.stream().filter(n -> predIds.contains(n.getId()))
                    .peek(n -> {
                        if (schemaMap.containsKey(n.getId())) {
                            n.setSchema(schemaMap.get(n.getId()));
                        }
                        if (outSchemaMap.containsKey(n.getId())) {
                            n.setOutputSchema(outSchemaMap.get(n.getId()));
                        }
                    })
                    .collect(Collectors.toList());
            nodes.add(target);
        }

        List<Edge> edges = dag.getEdges();
        if (CollectionUtils.isNotEmpty(edges)) {
            edges = edges.stream().filter(e -> (predIds.contains(e.getTarget()) || predIds.contains(e.getSource())) && !e.getSource().equals(this.getId())).collect(Collectors.toList());
            Edge edge = new Edge(this.getId(), target.getId());
            edges.add(edge);
        }

        ObjectId taskId = this.getDag().getTaskId();
        dag.setNodes(nodes);
        dag.setEdges(edges);
        DAG build = DAG.build(dag);
        build.getNodes().forEach(node -> node.getDag().setTaskId(taskId));

        TaskDto taskDto = service.getTaskById(taskId == null ? null : taskId.toHexString());

        TaskDto taskDtoCopy = new TaskDto();
        BeanUtils.copyProperties(taskDto, taskDtoCopy);
        taskDtoCopy.setStatus(TaskDto.STATUS_WAIT_RUN);
        taskDtoCopy.setSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);
        taskDtoCopy.setDag(build);
        taskDtoCopy.setId(Optional.of(new ObjectId(taskDto.getTransformTaskId())).orElseGet(ObjectId::new));
        taskDtoCopy.setName(taskDto.getName() + "(100)");
        taskDtoCopy.setParentSyncType(taskDto.getSyncType());

        List<Schema> result = Lists.newArrayList();
        List<MigrateJsResultVo> jsResult;
        try {
            jsResult = service.getJsResult(getId(), target.getId(), taskDtoCopy);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (CollectionUtils.isEmpty(jsResult)) {
            result = Lists.newArrayList();
        } else {
            Map<String, MigrateJsResultVo> removeMap = jsResult.stream()
                    .filter(n -> "REMOVE".equals(n.getOp()))
                    .collect(Collectors.toMap(MigrateJsResultVo::getFieldName, Function.identity(), (e1, e2) -> e1));

            Map<String, MigrateJsResultVo> convertMap = jsResult.stream()
                    .filter(n -> "CONVERT".equals(n.getOp()))
                    .collect(Collectors.toMap(MigrateJsResultVo::getFieldName, Function.identity(), (e1, e2) -> e1));

            Map<String, TapField> createMap = jsResult.stream()
                    .filter(n -> "CREATE".equals(n.getOp()))
                    .collect(Collectors.toMap(MigrateJsResultVo::getFieldName, MigrateJsResultVo::getTapField, (e1, e2) -> e1));

            Map<String, MigrateJsResultVo> setPkMap = jsResult.stream()
                    .filter(n -> "SET_PK".equals(n.getOp()))
                    .collect(Collectors.toMap(MigrateJsResultVo::getFieldName, Function.identity(), (e1, e2) -> e1));

            Map<String, MigrateJsResultVo> unSetPkMap = jsResult.stream()
                    .filter(n -> "UN_SET_PK".equals(n.getOp()))
                    .collect(Collectors.toMap(MigrateJsResultVo::getFieldName, Function.identity(), (e1, e2) -> e1));

            Map<String, TapIndex> addIndexMap = jsResult.stream()
                    .filter(n -> "ADD_INDEX".equals(n.getOp()))
                    .collect(Collectors.toMap(v -> v.getTapIndex().getName(), MigrateJsResultVo::getTapIndex, (e1, e2) -> e1));

            Map<String, TapIndex> removeIndexMap = jsResult.stream()
                    .filter(n -> "REMOVE_INDEX".equals(n.getOp()))
                    .collect(Collectors.toMap(v -> v.getTapIndex().getName(), MigrateJsResultVo::getTapIndex, (e1, e2) -> e1));

            List<Schema> inputSchema = getInputSchema().get(0);
            if (CollectionUtils.isEmpty(inputSchema)) {
                return null;
            }

            // js result data
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

                if (!unSetPkMap.isEmpty()) {
                    unSetPkMap.keySet().forEach(name -> {
                        TapField tapField = nameFieldMap.get(name);
                        if (tapField != null) {
                            tapField.setPrimaryKey(false);
                            tapField.setPrimaryKeyPos(null);
                        }
                    });
                }

                if (!setPkMap.isEmpty()) {
                    setPkMap.keySet().forEach(name -> {
                        TapField tapField = nameFieldMap.get(name);
                        if (tapField != null) {
                            tapField.setPrimaryKey(true);
                            tapField.setPrimaryKeyPos(tapTable.getMaxPKPos() + 1);
                        }
                    });
                }

                List<TapIndex> indexList = Optional.ofNullable(tapTable.getIndexList()).orElse(new ArrayList<>());
                if (!removeIndexMap.isEmpty()) {
                    indexList.removeIf(i -> removeIndexMap.containsKey(i.getName()));
                }

                if (!addIndexMap.isEmpty()) {
                    Set<String> existIndexNameSet = indexList.stream().map(TapIndex::getName).collect(Collectors.toSet());
                    addIndexMap.entrySet().removeIf(e -> existIndexNameSet.contains(e.getKey()));
                    indexList.addAll(addIndexMap.values());
                }
                tapTable.setIndexList(indexList);

                Schema jsSchema = PdkSchemaConvert.fromPdkSchema(tapTable);
                jsSchema.setDatabaseId(schema.getDatabaseId());

                List<Field> fields = jsSchema.getFields();
                Set<String> fieldNames = fields.stream().map(Field::getFieldName).collect(Collectors.toSet());
                Map<String, Field> originFieldMap = schema.getFields().stream().collect(Collectors.toMap(Field::getFieldName, f -> f));

                if (CollectionUtils.isNotEmpty(fields)) {
                    for (Field field : fields) {
                        Field originField = originFieldMap.get(field.getFieldName());
                        if (originField != null) {
                            originField.setAutoincrement(field.getAutoincrement());
                            originField.setDataType(field.getDataType());
                            originField.setIsNullable(field.getIsNullable());
                            originField.setColumnPosition(field.getColumnPosition());
                            originField.setTapType(field.getTapType());
                            originField.setPrimaryKeyPosition(field.getPrimaryKeyPosition());
                            originField.setPrimaryKey(field.getPrimaryKey());
                            BeanUtil.copyProperties(originField, field);

                            field.setId(new ObjectId().toHexString());
                            field.setDataTypeTemp(originField.getDataType());
                            field.setOriginalDataType(originField.getDataType());
                            field.setOriginalFieldName(originField.getOriginalFieldName());
                        }
                    }
                }

                List<TableIndex> indices = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(schema.getIndices())) {
                    for (TableIndex index : schema.getIndices()) {
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
                jsSchema.setIndices(indices);

                result.add(jsSchema);
            }
        }

        return result;
    }

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas, DAG.Options options) {
        //js节点的模型可以是直接虚拟跑出来的。 跑出来就是正确的模型，由引擎负责传值给tm
        if (CollectionUtils.isNotEmpty(schemas)) {
            return schemas;
        }

        List<Schema> result = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(inputSchemas)) {
            for (List<Schema> inputSchema : inputSchemas) {
                result.addAll(inputSchema);
            }
        }
        return result;
    }

    public MigrateJsProcessorNode() {
        super(NodeEnum.migrate_js_processor.name(), NodeCatalog.processor);
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

        schema.forEach(s -> {
            //s.setTaskId(taskId);
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
