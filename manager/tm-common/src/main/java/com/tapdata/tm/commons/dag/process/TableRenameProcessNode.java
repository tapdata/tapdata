package com.tapdata.tm.commons.dag.process;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.tapdata.tm.commons.base.convert.ObjectIdDeserialize.toObjectId;

@NodeType("table_rename_processor")
@Getter
@Setter
@Slf4j
public class TableRenameProcessNode extends MigrateProcessorNode {
    /**
     * 创建处理器节点
     *
     **/
    public TableRenameProcessNode() {
        super(NodeEnum.table_rename_processor.name(), NodeCatalog.processor);
    }

    /**
     * 源表名-新表名
     */
    private LinkedHashSet<TableRenameTableInfo> tableNames;


    public Map<String, TableRenameTableInfo> originalMap () {
        if (Objects.isNull(tableNames) || tableNames.isEmpty()) {
            return Maps.newLinkedHashMap();
        }

        return tableNames.stream().collect(Collectors.toMap(TableRenameTableInfo::getOriginTableName, Function.identity()));
    }

    public Map<String, TableRenameTableInfo> currentMap() {
        if (Objects.isNull(tableNames) || tableNames.isEmpty()) {
            return Maps.newLinkedHashMap();
        }

        return tableNames.stream().collect(Collectors.toMap(TableRenameTableInfo::getCurrentTableName, Function.identity()));
    }

    @Override
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas, DAG.Options options) {
        if (CollectionUtils.isEmpty(inputSchemas)) {
            return Lists.newArrayList();
        }

        if (Objects.isNull(tableNames) || tableNames.isEmpty()) {
            return inputSchemas.get(0);
        }

        inputSchemas.get(0).forEach(schema -> {
            String originalName = schema.getOriginalName();
            if (originalMap().containsKey(originalName)) {
                String currentTableName = originalMap().get(originalName).getCurrentTableName();
                schema.setName(currentTableName);
                schema.setOriginalName(currentTableName);
                schema.setAncestorsName(originalName);
                //schema.setDatabaseId(null);
                //schema.setQualifiedName(MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), getId()));
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
