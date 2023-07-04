package com.tapdata.tm.commons.dag.process;

import com.google.common.collect.Lists;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import com.tapdata.tm.commons.util.RemoveBracketsUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.tapdata.tm.commons.base.convert.ObjectIdDeserialize.toObjectId;

@NodeType("migrate_field_mod_type_filter_processor")
@Getter
@Setter
@Slf4j
public class MigrateTypeFilterProcessorNode extends MigrateProcessorNode {

    /**
     * 创建处理器节点
     *
     */
    public MigrateTypeFilterProcessorNode() {
        super(NodeEnum.migrate_field_mod_type_filter_processor.name(), NodeCatalog.processor);
    }

    private List<String> filterTypes;

    private  Map<String, Map<String, FieldInfo>> fieldTypeFilterMap = new HashMap<>();

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas, DAG.Options options) {
        if (CollectionUtils.isEmpty(inputSchemas)) {
            return Lists.newArrayList();
        }

        if (CollectionUtils.isEmpty(filterTypes)) {
            return inputSchemas.get(0);
        }

       // Map<String,FieldTypeInfo>tableFieldInfoMap = fieldTypes.stream().collect(Collectors.toMap(FieldTypeInfo::getOriginTableName, Function.identity(), (e1, e2)->e2));

        inputSchemas.get(0).forEach(schema -> {
            List<Field> fields = schema.getFields();
            String ancestorsName = schema.getName();
                Map<String, FieldInfo> filterFields = new HashMap<>();
                for (Field field : fields) {
                    Boolean show = filterTypes.contains(RemoveBracketsUtil.removeBrackets(field.getDataType()));
                    if (Objects.nonNull(show) && show) {
                        field.setDeleted(true);
                        FieldInfo fieldInfo = new FieldInfo(field.getFieldName(),null,false,field.getDataType());
                        filterFields.put(field.getFieldName(),fieldInfo);
                    }

                }
            fieldTypeFilterMap.put(ancestorsName,filterFields);
        });
        return inputSchemas.get(0);
    }

    @Override
    protected List<Schema> loadSchema(List<String> includes) {
        return null;
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

}
