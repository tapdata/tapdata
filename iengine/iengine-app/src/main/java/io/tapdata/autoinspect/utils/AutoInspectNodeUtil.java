package io.tapdata.autoinspect.utils;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.autoinspect.entity.AutoInspectProgress;
import com.tapdata.tm.autoinspect.utils.AutoInspectUtil;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.AutoInspectNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;

import java.util.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/22 21:32 Create
 */
public class AutoInspectNodeUtil {
    public static AutoInspectProgress parse(Map<String, Object> taskAttrs) {
        if (null != taskAttrs) {
            Object autoInspectProgress = taskAttrs.get("autoInspectProgress");
            if (autoInspectProgress instanceof Map) {
                String jsonStr = JSON.toJSONString(autoInspectProgress);
                return JSON.parseObject(jsonStr, AutoInspectProgress.class);
            }
        }
        return null;
    }

    public static void generateAutoInspectNode(TaskDto task, List<Node> nodes, List<Edge> edges, Map<String, Node<?>> nodeMap) {
        ObsLogger logger = ObsLoggerFactory.getInstance().getObsLogger(task);

        if (!task.isAutoInspect()) {
            return;
        }
        if (!task.isCanOpenInspect()) {
            logger.warn("Can not open AutoInspect: {}", task.getName());
            return;
        }
        if (task.getIsOpenAutoDDL()) {
            logger.warn("Turn on automatic DDL to ignore AutoInspect: {}", task.getName());
            return;
        }

        LinkedList<DatabaseNode> targetNodes = task.getDag().getTargetNode();
        for (DatabaseNode targetNode : targetNodes) {
            if (Boolean.TRUE.equals(targetNode.getEnableDynamicTable())) {
                logger.warn("Enable dynamic table to ignore AutoInspect: {}", targetNode.getName());
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

            Optional.ofNullable(findOneSourceNode(logger, targetNode)).ifPresent(n -> {
                inspectNode.setFromNode(n);
                inspectNode.setToNode(targetNode);
                nodes.add(inspectNode);
                nodeMap.put(inspectNode.getId(), inspectNode);
                edges.add(new Edge(n.getId(), inspectNode.getId()));
            });
        }
    }

    private static DatabaseNode findOneSourceNode(ObsLogger logger, DatabaseNode targetNode) {
        List<Node<List<Schema>>> predecessors;
        Node<List<Schema>> lastNode = targetNode;
        Set<String> exists = new HashSet<>();
        while (!exists.contains(lastNode.getId())) {
            exists.add(lastNode.getId());
            predecessors = lastNode.predecessors();
            if (predecessors.isEmpty()) {
                if (targetNode.getId().equals(lastNode.getId())) {
                    logger.warn("AutoInspect filter no edge: {}", targetNode.getName());
                    return null;
                }
                if (lastNode instanceof DatabaseNode) {
                    return (DatabaseNode) lastNode;
                }
                logger.warn("AutoInspect filter no DataParentNode: {}", targetNode.getName());
                return null;
            } else if (predecessors.size() > 1) {
                logger.warn("AutoInspect filter multi-edge: {}", targetNode.getName());
                return null;
            }
            lastNode = predecessors.get(0);
        }

        logger.warn("AutoInspect filter repeat edge: {}", targetNode.getName());
        return null;
    }
}
