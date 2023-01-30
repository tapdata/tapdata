package com.tapdata.tm.commons.dag.process;

import com.google.common.collect.Lists;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.type.TapMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 多表合并原地更新模式调回产品（主从合并）
 */
@NodeType("merge_table_processor")
@Getter
@Setter
@Slf4j
public class MergeTableNode extends ProcessorNode {
    private List<MergeTableProperties> mergeProperties;

    public MergeTableNode() {
        super("merge_table_processor");
    }


    @Override
    protected Schema loadSchema(List<String> includes) {
        return null;
    }

    /**
     * 把多个属性里面的表，根据关联条件合并成一个宽表
     *
     * @return
     */
    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        Map<String, Schema> schemaMap = inputSchemas.stream().collect(Collectors.toMap(Schema::getQualifiedName, s -> s, (a1, a2) -> a1));

        Schema main = null;
        Set<String> inlineF = new HashSet<>();
        if (CollectionUtils.isNotEmpty(mergeProperties)) {
            for (MergeTableProperties mergeProperty : mergeProperties) {
                main = merge(schemaMap, mergeProperty, main, inlineF);
            }
        }

        String primaryTable = null;

        if (main != null) {
            List<Field> fields = main.getFields();
            if (CollectionUtils.isNotEmpty(fields)) {
                for (Field field : fields) {
                    if (inlineF.contains(field.getId())) {
                        continue;
                    }

                    if (primaryTable != null && !field.getTableName().equals(primaryTable)) {
                        if ((field.getPrimaryKey() != null && field.getPrimaryKey()) || (field.getPrimaryKeyPosition() != null && field.getPrimaryKeyPosition() != 0)) {
                            field.setPrimaryKey(false);
                            field.setPrimaryKeyPosition(0);
                        }
                    }

                    if (field.getPrimaryKey() != null && field.getPrimaryKey() && primaryTable == null) {
                        primaryTable = field.getTableName();
                    }
                }
            }
        }


        return super.mergeSchema(Lists.newArrayList(main),null, options);
    }

    private Schema merge(Map<String, Schema> schemaMap, MergeTableProperties mergeProperty, Schema main, Set<String> inlineF) {
        String id = mergeProperty.getId();
        Schema schema1 = schemaMap.get(getQualifiedNameByNodeId(this.getDag(), id));
        String targetPath = mergeProperty.getTargetPath();
        if (StringUtils.isNotBlank(targetPath)) {
            if (schema1 != null) {
                Field mapField = createMapField(targetPath, schema1.getName());
                List<Field> fields = schema1.getFields();
                for (Field field : fields) {
                    inlineF.add(field.getId());
                    field.setFieldName(mergeProperty.getTargetPath() + "." + field.getFieldName());
                    if ((field.getPrimaryKey() != null && field.getPrimaryKey()) || (field.getPrimaryKeyPosition() != null && field.getPrimaryKeyPosition() != 0)) {
                        field.setPrimaryKey(false);
                        field.setPrimaryKeyPosition(0);
                    }
                }
                fields.add(mapField);
            }
        }

        main = SchemaUtils.mergeSchema(Lists.newArrayList(main), schema1, false);

        List<MergeTableProperties> children = mergeProperty.getChildren();
        if (CollectionUtils.isNotEmpty(children)) {
            for (MergeTableProperties child : children) {
                main = merge(schemaMap, child, main, inlineF);
            }
        }

        return main;
    }

    public String getQualifiedNameByNodeId(DAG dag, String nodeId) {
        Node node = dag.getNode(nodeId);
        if (node == null) {
            return null;
        }

        if (node instanceof TableNode) {
            DataSourceConnectionDto dataSource = service.getDataSource(((TableNode) node).getConnectionId());
            String metaType = "table";
            if ("mongodb".equals(dataSource.getDatabase_type())) {
                metaType = "collection";
            }

            String tableName = ((TableNode) node).transformTableName(((TableNode) node).getTableName());
            return MetaDataBuilderUtils.generateQualifiedName(metaType, dataSource, tableName, getTaskId());
        } else if (node instanceof ProcessorNode) {
            return MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), nodeId, null, getTaskId());
        }
        return null;
    }

  /*  @Override
    protected Schema saveSchema(Collection<String> predecessors, String nodeId, Schema schema, DAG.Options options) {
        if (service == null) {
            return schema;
        }
        schema.setNodeId(getId());
        schema.setTaskId(taskId());
        List<Schema> result = service.createOrUpdateSchema(ownerId(), Collections.singletonList(schema), options, this);
        if (result != null && result.size() > 0)
            return result.get(0);
        return schema;
    }*/

    public Field createMapField(String name, String tableName) {
        Field field = new Field();
        field.setFieldName(name);
        field.setSource("job_analyze");
        field.setDataType("Map");
        field.setId(new ObjectId().toHexString());
        field.setTableName(tableName);
        field.setOriginalFieldName(name);
        TapMap tapMap = new TapMap();
        tapMap.setType((byte) 4);
        field.setTapType(JsonUtil.toJson(tapMap));
        return field;
    }


    @Override
    public boolean validate() {
        return validateMergeProperties(mergeProperties);
    }

    public boolean validateMergeProperties(List<MergeTableProperties> mergeProperties) {
        if (CollectionUtils.isNotEmpty(mergeProperties)) {
            for (MergeTableProperties mergeProperty : mergeProperties) {
                String targetPath = mergeProperty.getTargetPath();
                if (StringUtils.isNotBlank((targetPath))) {
                    int num = 0;
                    for (int i = 0; i < targetPath.length(); i++) {
                        char c = targetPath.charAt(i);
                        if (c == '.') {
                            if (++num >= 2) {
                                return false;
                            }
                        }
                    }
                }

                List<MergeTableProperties> children = mergeProperty.getChildren();
                if (CollectionUtils.isNotEmpty(children)) {
                    boolean validate = validateMergeProperties(children);
                    if (!validate) {
                        return false;
                    }

                }
            }
        }

        return true;
    }

    @Override
    public void fieldDdlEvent(TapDDLEvent event) {

    }
}
