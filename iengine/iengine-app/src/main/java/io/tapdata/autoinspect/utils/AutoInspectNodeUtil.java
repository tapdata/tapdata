package io.tapdata.autoinspect.utils;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.exception.AutoInspectException;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.AutoInspectNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.TaskDto;
import lombok.NonNull;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/22 21:32 Create
 */
public class AutoInspectNodeUtil {

    public static AutoInspectNode firstAutoInspectNode(TaskDto task) {
        if (task.isAutoInspect()) {
            if (task.getCanOpenInspect() == null || !task.getCanOpenInspect()) {
                throw AutoInspectException.canNotOpen(task.getName());
            }
            if (task.getIsOpenAutoDDL()) {
                throw AutoInspectException.openAutoDDL(task.getName());
            }

            LinkedList<DatabaseNode> targetNodes = task.getDag().getTargetNode();
            for (DatabaseNode targetNode : targetNodes) {
                if (Boolean.TRUE.equals(targetNode.getEnableDynamicTable())) {
                    throw AutoInspectException.enableDynamicTable(task.getName(), targetNode.getName());
                }

                return generateAutoInspectNode(targetNode);
            }
        }
        return null;
    }

    private static AutoInspectNode generateAutoInspectNode(DatabaseNode targetNode) {
        AutoInspectNode inspectNode = new AutoInspectNode();
        inspectNode.setId(UUID.randomUUID().toString());
        inspectNode.setType(AutoInspectConstants.NODE_TYPE);
        inspectNode.setName(targetNode.getName() + "-" + AutoInspectConstants.MODULE_NAME);
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

        inspectNode.setFromNode(findOneSourceNode(targetNode));
        inspectNode.setToNode(targetNode);
        return inspectNode;
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
                    throw AutoInspectException.filterSourceNode(targetNode.getName(), "no edge");
                }
                if (lastNode instanceof DatabaseNode) {
                    return (DatabaseNode) lastNode;
                }
                throw AutoInspectException.filterSourceNode(targetNode.getName(), "no DataParentNode");
            } else if (predecessors.size() > 1) {
                throw AutoInspectException.filterSourceNode(targetNode.getName(), "multi-edge");
            }
            lastNode = predecessors.get(0);
        }

        throw AutoInspectException.filterSourceNode(targetNode.getName(), "repeat edge");
    }

    public static Map<String, Connections> getConnectionsByIds(@NonNull ClientMongoOperator clientMongoOperator, @NonNull Set<String> ids) {
        Query query = Query.query(Criteria.where("_id").in(ids));
        query.fields().exclude("response_body").exclude("schema");
        List<Connections> connections = clientMongoOperator.find(query, ConnectorConstant.CONNECTION_COLLECTION + "/listAll", Connections.class);

        Map<String, Connections> retMap = new HashMap<>();
        if (null != connections) {
            connections.forEach(conn -> {
                retMap.put(conn.getId(), conn);
            });
        }
        return retMap;
    }
}
