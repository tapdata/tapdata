package com.tapdata.tm.dag.util;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataService;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.dataflow.dto.Stage;
import io.github.openlg.graphlib.Graph;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/9 下午3:37
 */
public class DAGUtils {

    static Logger logger = LoggerFactory.getLogger(DAGUtils.class);

    /**
     * 根据 Stage 列表构建 DAG
     *
     * @param ownerId DAG 所属用户ID
     * @param stages 节点
     * @param taskId 任务id
     * @return
     */
    public static DAG build(String ownerId, ObjectId taskId, List<Stage> stages, DAGDataService dagDataService) {

        Graph<Node, Edge> graph = new Graph<>();
        DAG dag = new DAG(graph);
        dag.setOwnerId(ownerId);
        dag.setTaskId(taskId);

        stages.forEach(stage -> {

            Node node = buildNode(stage);
            if (node == null) {
                logger.error("Build node({}) failed", stage.getId());
                return;
            }
            node.setGraph(graph);
            node.setDag(dag);
            node.setService(dagDataService);

            try {
                BeanUtils.copyProperties(stage, node);
            } catch (Exception e) {
                logger.error("Init node({}) properties failed", stage.getId(), e);
                return;
            }
            try {
                node.afterProperties();
            } catch (Exception e) {
                logger.error("Execute node({}) init method failed, node type {}", stage.getId(), node.getClass().getName(), e);
                return;
            }

            graph.setNode(stage.getId(), node);

            if (stage.getInputLanes() != null && stage.getInputLanes().size() > 0) {
                stage.getInputLanes().forEach(input -> graph.setEdge(input, stage.getId()));
            }
            if (stage.getOutputLanes() != null && stage.getOutputLanes().size() > 0) {
                stage.getOutputLanes().forEach(output -> graph.setEdge(stage.getId(), output));
            }
        });

        return dag;
    }

    private static Node buildNode(Stage stage) {
        if(!DAG.nodeMapping.containsKey(stage.getType())) {
            logger.error("Not found node type {}", stage.getType());
            return null;
        }
        Class<? extends Node> nodeClass = DAG.nodeMapping.get(stage.getType());
        try {
            return nodeClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            logger.error("Init node object failed", e);
        }
        return null;
    }

}
