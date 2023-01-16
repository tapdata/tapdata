package com.tapdata.tm.commons.dag.process;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

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

    private String prefix;
    private String suffix;
    private String replaceBefore;
    private String replaceAfter;
    private String transferCase;



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
    public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas) {
        if (CollectionUtils.isEmpty(inputSchemas)) {
            return Lists.newArrayList();
        }

        if (Objects.isNull(tableNames) || tableNames.isEmpty()) {
            return inputSchemas.get(0);
        }

        inputSchemas.get(0).forEach(schema -> {
            String originalName = schema.getAncestorsName();
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


    @Override
    public void fieldDdlEvent(TapDDLEvent event) throws Exception {
        if (event instanceof TapCreateTableEvent) {
            String tableName = event.getTableId();

            List<Node> sources = this.getDag().getSources();
            Node node = sources.get(0);
            LinkedList<TableRenameProcessNode> linkedList = new LinkedList<>();
            while (!node.getId().equals(this.getId())) {
                if (node instanceof TableRenameProcessNode) {
                    linkedList.add((TableRenameProcessNode) node);
                }
                List successors = node.successors();
                if (CollectionUtils.isEmpty(successors)) {
                    break;
                }
                node = (Node) successors.get(0);
            }
            TableRenameTableInfo tableInfo = null;
            if (CollectionUtils.isNotEmpty(linkedList)) {
                for (TableRenameProcessNode node1 : linkedList) {
                    Map<String, TableRenameTableInfo> tableRenameTableInfoMap = node1.originalMap();
                    TableRenameTableInfo tableInfo1 = tableRenameTableInfoMap.get(tableName);
                    if (tableInfo1 != null) {
                        tableInfo = tableInfo1;
                    }
                }
            }

            String lastTableName = tableName;
            if (tableInfo != null) {
                lastTableName = tableInfo.getCurrentTableName();
            }

            tableInfo = new TableRenameTableInfo(tableName, lastTableName, convertTableName(lastTableName));
            tableNames.add(tableInfo);
        } else if (event instanceof TapDropTableEvent) {
            String tableName = event.getTableId();
            for (TableRenameTableInfo tableInfo : tableNames) {
                if (tableInfo.getOriginTableName().equals(tableName)) {
                    tableNames.remove(tableInfo);
                    break;
                }
            }
        }
    }

    private String convertTableName(String tableName) {
        if (StringUtils.isNotBlank(prefix)) {
            tableName = prefix + tableName;
        }

        if (StringUtils.isNotBlank(suffix)) {
            tableName = tableName + suffix;
        }

        if (StringUtils.isNotBlank(replaceBefore) && replaceAfter != null) {
            tableName = tableName.replace(replaceBefore, replaceAfter);
        }

        if (StringUtils.isNotBlank(transferCase)) {
            if (transferCase.equals("toUpperCase")) {
                tableName = tableName.toUpperCase(Locale.ROOT);
            } else if (transferCase.equals("toLowerCase")) {
                tableName = tableName.toLowerCase(Locale.ROOT);
            }
        }
        return tableName;
    }
}
