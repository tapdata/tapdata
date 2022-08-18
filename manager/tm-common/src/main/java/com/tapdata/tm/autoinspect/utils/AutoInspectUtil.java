package com.tapdata.tm.autoinspect.utils;

import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.AutoInspectNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.logger.TapLogger;

import java.util.*;

public class AutoInspectUtil {
    private static final String TAG = AutoInspectUtil.class.getSimpleName();

    public static void generateAutoInspectNode(TaskDto task, List<Node> nodes, List<Edge> edges, Map<String, Node<?>> nodeMap) {
        if (!task.isAutoInspect()) {
            return;
        }
        //todo: check AutoInspect capabilitys
//        if (!task.isCanOpenInspect()) {
//            TapLogger.warn(TAG, "can not open AutoInspect: {}", task.getName());
//            return;
//        }
//        if (task.getIsOpenAutoDDL()) {
//            TapLogger.warn("Turn on automatic DDL to ignore AutoInspect: {}", task.getName());
//            return;
//        }

        LinkedList<DatabaseNode> targetNodes = task.getDag().getTargetNode();
        for (DatabaseNode targetNode : targetNodes) {
            if (isEnableDynamicTable(targetNode)) {
                TapLogger.warn(TAG, "Enable dynamic table to ignore AutoInspect: {}", targetNode.getName());
                continue;
            }

            AutoInspectNode inspectNode = new AutoInspectNode();
            inspectNode.setId(UUID.randomUUID().toString());
            inspectNode.setType("auto_inspect");
            inspectNode.setName(targetNode.getName() + "-AutoInspect");
            inspectNode.setConnectionId(targetNode.getConnectionId());
            inspectNode.setDatabaseType(targetNode.getDatabaseType());
            inspectNode.setAttrs(targetNode.getAttrs());
            inspectNode.setSyncObjects(targetNode.getSyncObjects());
            inspectNode.setInitialConcurrentWriteNum(targetNode.getInitialConcurrentWriteNum());
            inspectNode.setDag(targetNode.getDag());
            inspectNode.setGraph(targetNode.getGraph());
            inspectNode.setSchema(targetNode.getSchema());
            inspectNode.setEnableDynamicTable(targetNode.getEnableDynamicTable());
            inspectNode.setTargetNodeId(targetNode.getId());

            Optional.ofNullable(findOneSourceNode(targetNode)).ifPresent(n -> {
                inspectNode.setFromNode(n);
                inspectNode.setToNode(targetNode);
                nodes.add(inspectNode);
                nodeMap.put(inspectNode.getId(), inspectNode);
                edges.add(new Edge(n.getId(), inspectNode.getId()));
            });
        }
    }

    private static DatabaseNode findOneSourceNode(DatabaseNode targetNode) {
        List<Node<List<Schema>>> predecessors;
        Node<List<Schema>> lastNode = targetNode;
        Set<String> exists = new HashSet<>();
        while (!exists.contains(lastNode.getId())) {
            exists.add(lastNode.getId());
            predecessors = lastNode.predecessors();
            if (predecessors.isEmpty()) {
                if (targetNode.getId().equals(lastNode.getId())) {
                    TapLogger.warn(TAG, "AutoInspect filter no edge: {}", targetNode.getName());
                    return null;
                }
                if (lastNode instanceof DatabaseNode) {
                    return (DatabaseNode) lastNode;
                }
                TapLogger.warn(TAG, "AutoInspect filter no DataParentNode: {}", targetNode.getName());
                return null;
            } else if (predecessors.size() > 1) {
                TapLogger.warn(TAG, "AutoInspect filter multi-edge: {}", targetNode.getName());
                return null;
            }
            lastNode = predecessors.get(0);
        }

        TapLogger.warn(TAG, "AutoInspect filter repeat edge: {}", targetNode.getName());
        return null;
    }

    private static boolean isEnableDynamicTable(DatabaseNode node) {
        return Boolean.TRUE.equals(node.getEnableDynamicTable());
    }
}
