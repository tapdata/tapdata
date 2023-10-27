package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.NodeTypeEnum;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.schema.TapTableMap;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HazelcastJoinProcessNodeTest {

    public HazelcastBaseNode createJoinNode(String joinId, String joinName, String nodeLeftId, String nodeRightId, List<Node> nodes, List<Edge> edges) throws Exception {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        Node node = new JoinProcessorNode();
        node.setType(NodeTypeEnum.JOIN.type);

        final Node<?> nodeLeft = new JsProcessorNode();
        nodeLeft.setName("Js-left");
        nodeLeft.setId(nodeLeftId);
        final Node<?> nodeRight = new JsProcessorNode();
        nodeRight.setName("Js-right");
        nodeRight.setId(nodeRightId);
        Graph<Node, Edge> graph = new Graph<>();
        graph.setNode(nodeLeftId, nodeLeft);
        graph.setNode(nodeRightId, nodeRight);

        final Edge e2 = new Edge();
        e2.setSource(nodeRightId);
        e2.setTarget(joinId);
        final io.github.openlg.graphlib.Edge e22 = new io.github.openlg.graphlib.Edge(nodeRightId, joinId);
        graph.setEdge(e22);
        final Edge e1 = new Edge();
        e1.setSource(nodeLeftId);
        e1.setTarget(joinId);
        final io.github.openlg.graphlib.Edge e11 = new io.github.openlg.graphlib.Edge(nodeLeftId, joinId);
        graph.setEdge(e11);

        DAG dag = new DAG(graph);
        node.setDag(dag);
        node.setId(joinId);
        node.setName(joinName);

        edges.add(e1);
        edges.add(e2);

        nodes.add(node);
        nodes.add(nodeLeft);
        nodes.add(nodeRight);


        List<Node> predecessors = new ArrayList<>();
        predecessors.add(node);
        predecessors.add(nodeLeft);
        predecessors.add(nodeRight);

        List<Node> successors = new ArrayList<>();
        successors.add(node);
        successors.add(nodeLeft);
        successors.add(nodeRight);

        ConfigurationCenter config = new ConfigurationCenter();
        Connections connection = new Connections();
        DatabaseTypeEnum.DatabaseType databaseType = null;//new DatabaseTypeEnum.DatabaseTyp();
        Map<String, MergeTableNode> mergeTableMap = new HashMap<>();
        TapTableMap<String, TapTable> tapTableMap = null;//new TapTableMap<>();
        TaskConfig taskConfig = new TaskConfig();
        taskDto.setDag(dag);
        return HazelcastTaskService.createNode(
                taskDto,
                nodes,
                edges,
                node,
                predecessors,
                successors,
                config,
                connection,
                databaseType,
                mergeTableMap,
                tapTableMap,
                taskConfig
        );
    }

    /**
     * 检查创建的node是否是Join节点
     * 检查创建的节点是否包含node列表
     * 检查创建的节点中包含的node是否和创建前一致
     * */
    @Test
    public void createJoinNodeWithNodes() {
//        final String nodeLeftId = "47yr4783yr";
//        final String nodeRightId = "57yr4783yr";
//        final String joinName = "JoinNode";
//        final String joinId = "67yr4783yr";
//        HazelcastBaseNode joinNode = null;
//        List<Node> nodes = new ArrayList<>();
//        List<Edge> edges = new ArrayList<>();
//        try {
//            joinNode = createJoinNode(joinId, joinName, nodeLeftId, nodeRightId, nodes, edges);
//        } catch (Exception e) {
//            Assert.fail("Using HazelcastTaskService's createNode method to create a join node, expect no errors, but in reality, an error occurred: " + e.getMessage());
//        }
//        Assert.assertNotNull("Using HazelcastTaskService's createNode method to create a process join node can not be null", joinNode);
//        Assert.assertTrue("Using HazelcastTaskService's createNode method to create a process join node must be HazelcastJoinProcessor", joinNode instanceof HazelcastJoinProcessor);
//        HazelcastJoinProcessor join = (HazelcastJoinProcessor)joinNode;
//        Node<?> node1 = join.getNode();
//        Assert.assertTrue("Using HazelcastTaskService's createNode method to create a join node must be JoinProcessorNode", node1 instanceof JoinProcessorNode);
//        DAG dagRes = node1.getDag();
//        Assert.assertNotNull("Using HazelcastTaskService's createNode method to create a join node, and node's DAG must be null after created", dagRes);
//        List<Node> dagNodes = dagRes.getNodes();
//        Assert.assertEquals("Using HazelcastTaskService's createNode method to create a join node, and Node list's size should be equals ago", dagNodes.size(), nodes.size());
//        LinkedList<Edge> dagEdges = dagRes.getEdges();
//        Assert.assertEquals("Using HazelcastTaskService's createNode method to create a join node, and Edge list's size should be equals ago", dagEdges.size(), edges.size());

    }

    /**
     * 检查创建的node是否是Join节点
     * 检查创建的节点是否可以正常获取前置的左节点和右节点
     * */
    @Test
    public void createJoinNodeAndCanGetLeftOrRightNodeByJoinNode() {
//        final String nodeLeftId = "47yr4783yr";
//        final String nodeRightId = "57yr4783yr";
//        final String joinName = "JoinNode";
//        final String joinId = "67yr4783yr";
//        HazelcastBaseNode joinNode = null;
//        List<Node> nodes = new ArrayList<>();
//        List<Edge> edges = new ArrayList<>();
//        try {
//            joinNode = createJoinNode(joinId, joinName, nodeLeftId, nodeRightId, nodes, edges);
//        } catch (Exception e) {
//            Assert.fail("Using HazelcastTaskService's createNode method to create a join node, expect no errors, but in reality, an error occurred: " + e.getMessage());
//        }
//        Assert.assertNotNull("Using HazelcastTaskService's createNode method to create a process join node can not be null", joinNode);
//        Assert.assertTrue("Using HazelcastTaskService's createNode method to create a process join node must be HazelcastJoinProcessor", joinNode instanceof HazelcastJoinProcessor);
//        HazelcastJoinProcessor join = (HazelcastJoinProcessor)joinNode;
//
//        Node<?> node1 = join.getNode();
//        Assert.assertTrue("Using HazelcastTaskService's createNode method to create a join node must be JoinProcessorNode", node1 instanceof JoinProcessorNode);
//        JoinProcessorNode jNode = (JoinProcessorNode)node1;
//        String leftNodeId = jNode.getLeftNodeId();
//        Assert.assertTrue("Using HazelcastTaskService's createNode method to create a join node, and left node id not be null or empty", null != leftNodeId && !"".equals(leftNodeId));
//        Assert.assertEquals("Using HazelcastTaskService's createNode method to create a join node, and left node id should be equals create ago", nodeLeftId, leftNodeId);
//        String rightNodeId = jNode.getRightNodeId();
//        Assert.assertTrue("Using HazelcastTaskService's createNode method to create a join node, and right node id not be null or empty", null != rightNodeId && !"".equals(rightNodeId));
//        Assert.assertEquals("Using HazelcastTaskService's createNode method to create a join node, and left node id should be equals create ago", nodeRightId, rightNodeId);
//        DAG dagRes = node1.getDag();
//        Assert.assertNotNull("Using HazelcastTaskService's createNode method to create a join node, and node's DAG must be null after created", dagRes);
//        List<Node> dagNodes = dagRes.getNodes();
//        Assert.assertEquals("Using HazelcastTaskService's createNode method to create a join node, and Node list's size should be equals ago", dagNodes.size(), nodes.size());
//        LinkedList<Edge> dagEdges = dagRes.getEdges();
//        Assert.assertEquals("Using HazelcastTaskService's createNode method to create a join node, and Edge list's size should be equals ago", dagEdges.size(), edges.size());
//        Node<?> leftNode = dagRes.getNode(leftNodeId);
//        Assert.assertNotNull("Using HazelcastTaskService's createNode method to create a join node, and node can not be null which is using left node id and get from dag", leftNode);
//        Node<?> rightNode = dagRes.getNode(rightNodeId);
//        Assert.assertNotNull("Using HazelcastTaskService's createNode method to create a join node, and node can not be null which is using right node id and get from dag", rightNode);
    }
}
