package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.tapdata.tm.commons.base.convert.ObjectIdDeserialize.toObjectId;

public abstract class MigrateProcessorNode extends Node<List<Schema>> {
    /**
     * 创建处理器节点
     *
     **/
    public MigrateProcessorNode(String type, NodeCatalog catalog) {
        super(type, catalog);
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

    protected String getConnectId() {
        AtomicReference<String> connectionId = new AtomicReference<>("");

        getSourceNode().stream().findFirst().ifPresent(node -> connectionId.set(node.getConnectionId()));
        return connectionId.get();
    }
}
