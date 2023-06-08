package com.tapdata.tm.commons.dag.process;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.Operation;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import com.tapdata.tm.commons.util.CapitalizedEnum;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.tapdata.tm.commons.base.convert.ObjectIdDeserialize.toObjectId;

@NodeType("migrate_field_rename_processor")
@Getter
@Setter
@Slf4j
public class MigrateFieldRenameProcessorNode extends MigrateProcessorNode {
    /**
     * 创建处理器节点
     *
     **/
    public MigrateFieldRenameProcessorNode() {
        super(NodeEnum.migrate_field_rename_processor.name(), NodeCatalog.processor);
    }

    private LinkedList<TableFieldInfo> fieldsMapping;

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas, DAG.Options options) {

        supplementFieldMapping();
        if (CollectionUtils.isEmpty(inputSchemas)) {
            return Lists.newArrayList();
        }

        if (CollectionUtils.isEmpty(fieldsMapping)) {
            return inputSchemas.get(0);
        }

        Map<String, TableFieldInfo> tableFieldInfoMap = fieldsMapping.stream().collect(Collectors.toMap(TableFieldInfo::getOriginTableName, Function.identity(), (e1,e2)->e2));

        inputSchemas.get(0).forEach(schema -> {
            //schema.setDatabaseId(null);
            //schema.setQualifiedName(MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), getId()));

            List<Field> fields = schema.getFields();
            String ancestorsName = schema.getAncestorsName();
            if (tableFieldInfoMap.containsKey(ancestorsName)) {
                Map<String, Boolean> showMap = tableFieldInfoMap.get(ancestorsName).getFields()
                        .stream().collect(Collectors.toMap(FieldInfo::getSourceFieldName, FieldInfo::getIsShow));

                Map<String, String> fieldMap = tableFieldInfoMap.get(ancestorsName).getFields()
                        .stream().collect(Collectors.toMap(FieldInfo::getSourceFieldName, FieldInfo::getTargetFieldName));

                for (Field field : fields) {
                    String originalFieldName = field.getOriginalFieldName();
                    String fieldName = field.getFieldName();
                    Boolean show = showMap.get(originalFieldName);
                    if (Objects.nonNull(show) && !show) {
                        field.setDeleted(true);
                    } else if (fieldMap.containsKey(fieldName)) {
                        field.setFieldName(fieldMap.get(fieldName));
                    }

                }
            }
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


    private void supplementFieldMapping() {
            if (CollectionUtils.isNotEmpty(fieldsMapping)) {
                LinkedList<Node<?>> preNodes = getDag().getPreNodes(this.getId());
                if (CollectionUtils.isEmpty(preNodes)) {
                    return;
                }
                Node previousNode = preNodes.getLast();

                List<MetadataInstancesDto> metaList = service.findByNodeId(previousNode.getId());
                Map<String, List<com.tapdata.tm.commons.schema.Field>> fieldMap = metaList.stream()
                        .collect(Collectors.toMap(MetadataInstancesDto::getAncestorsName, MetadataInstancesDto::getFields));
                fieldsMapping.forEach(table -> {
                    Operation operation = table.getOperation();
                    LinkedList<FieldInfo> fields = table.getFields();

                    List<String> fieldNames = Lists.newArrayList();
                    if (CollectionUtils.isNotEmpty(fields)) {
                        fieldNames = fields.stream().map(FieldInfo::getSourceFieldName).collect(Collectors.toList());
                    }

                    List<String> hiddenFields = table.getFields().stream().filter(t -> !t.getIsShow())
                            .map(FieldInfo::getSourceFieldName)
                            .collect(Collectors.toList());

                    List<com.tapdata.tm.commons.schema.Field> tableFields = fieldMap.get(table.getOriginTableName());
                    if (CollectionUtils.isNotEmpty(tableFields)) {
                        for (com.tapdata.tm.commons.schema.Field field : tableFields) {
                            String targetFieldName = field.getFieldName();
                            if (!fieldNames.contains(targetFieldName)) {
                                if (CollectionUtils.isNotEmpty(hiddenFields) && hiddenFields.contains(targetFieldName)) {
                                    continue;
                                }

                                if (StringUtils.isNotBlank(operation.getPrefix())) {
                                    targetFieldName = operation.getPrefix().concat(targetFieldName);
                                }
                                if (StringUtils.isNotBlank(operation.getSuffix())) {
                                    targetFieldName = targetFieldName.concat(operation.getSuffix());
                                }
                                if (StringUtils.isNotBlank(operation.getCapitalized())) {
                                    if (CapitalizedEnum.fromValue(operation.getCapitalized()) == CapitalizedEnum.UPPER) {
                                        targetFieldName = StringUtils.upperCase(targetFieldName);
                                    } else {
                                        targetFieldName = StringUtils.lowerCase(targetFieldName);
                                    }
                                }
                                FieldInfo fieldInfo = new FieldInfo(field.getFieldName(), targetFieldName, true, "system");
                                fields.add(fieldInfo);
                            }
                        }
                    }
                });
            }
        }
}
