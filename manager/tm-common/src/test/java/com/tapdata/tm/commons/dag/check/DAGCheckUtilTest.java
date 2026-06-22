package com.tapdata.tm.commons.dag.check;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.task.dto.Message;
import io.github.openlg.graphlib.Graph;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DAGCheckUtilTest {
    @Test
    public void testSetNodeToDisabled(){
        Node<?> node = new JoinProcessorNode();
        Map<String, Object> attr = new HashMap<>();
        attr.put("disabled", false);
        node.setAttrs(attr);
        DAGCheckUtil.setNodeToDisabled(node);
        Assertions.assertTrue( node.isDisabled());
        Assertions.assertNotNull(node.getAttrs());
        Assertions.assertTrue(node.getAttrs().containsKey("disabled"));
        Assertions.assertEquals(Boolean.TRUE, node.getAttrs().get("disabled"));
    }

    @Test
    public void testSetNodeToDisabledNullNode(){
        Node<?> node = null;
        DAGCheckUtil.setNodeToDisabled(node);
        Assertions.assertNull(node);
    }

    @Test
    public void testSetNodeToDisabledNullAttrMap(){
        Node<?> node = new JoinProcessorNode();
        node.setAttrs(null);
        DAGCheckUtil.setNodeToDisabled(node);
        Assertions.assertTrue(node.isDisabled());
        Assertions.assertNotNull(node.getAttrs());
        Assertions.assertTrue(node.getAttrs().containsKey("disabled"));
        Assertions.assertEquals(Boolean.TRUE, node.getAttrs().get("disabled"));
    }

    @Test
    public void testSetNodeToDisabledEmptyAttrMap(){
        Node<?> node = new JoinProcessorNode();
        node.setAttrs(new HashMap<>());
        DAGCheckUtil.setNodeToDisabled(node);
        Assertions.assertTrue(node.isDisabled());
        Assertions.assertNotNull(node.getAttrs());
        Assertions.assertTrue(node.getAttrs().containsKey("disabled"));
        Assertions.assertEquals(Boolean.TRUE, node.getAttrs().get("disabled"));
    }

    @Test
    public void testCheckJoinNode(){
        List<Edge> edges = new ArrayList<>();
        List<Message> messageList = new ArrayList<>();
        List<Node> preNodes = new ArrayList<>();
        JoinProcessorNode node = getJoinNode(edges, preNodes);
        node.setDisabled(false);
        node.setAttrs(new HashMap<>());

        Node leftNode = preNodes.get(0);
        leftNode.setDisabled(false);
        leftNode.setAttrs(new HashMap<>());
        node.setLeftNodeId(leftNode.getId());

        Node rightNode = preNodes.get(1);
        rightNode.setDisabled(false);
        rightNode.setAttrs(new HashMap<>());
        node.setRightNodeId(rightNode.getId());
        DAGCheckUtil.checkJoinNode(node, edges, messageList);

        Assertions.assertEquals(0, messageList.size());
        Assertions.assertFalse(node.disabledNode());
    }

    @Test
    public void testCheckJoinNodeDisabledNode(){
        List<Edge> edges = new ArrayList<>();
        List<Message> messageList = new ArrayList<>();
        List<Node> preNodes = new ArrayList<>();
        JoinProcessorNode node = getJoinNode(edges, preNodes);

        node.setDisabled(true);
        Map<String, Object> attr = new HashMap<>();
        attr.put("disabled", true);
        node.setAttrs(attr);

        List<Edge> methodEdges = new ArrayList<>(edges);
        DAGCheckUtil.checkJoinNode(node, methodEdges, messageList);
        DAGCheckUtil.checkJoinNode(node, edges, messageList);

        Assertions.assertEquals(0, messageList.size());
        Assertions.assertTrue(node.disabledNode());
    }

    @Test
    public void testCheckJoinNodeNullNode(){
        List<Edge> edges = new ArrayList<>();
        List<Message> messageList = new ArrayList<>();
        JoinProcessorNode node = null;
        List<Edge> methodEdges = new ArrayList<>(edges);
        DAGCheckUtil.checkJoinNode(node, methodEdges, messageList);
        Assertions.assertNull(node);
        Assertions.assertEquals(0, messageList.size());
    }

    @Test
    public void testCheckJoinNodeNullEdges(){
        List<Edge> edges = new ArrayList<>();
        List<Message> messageList = new ArrayList<>();
        List<Node> preNodes = new ArrayList<>();
        JoinProcessorNode node = getJoinNode(edges, preNodes);
        List<Edge> methodEdges = null;
        DAGCheckUtil.checkJoinNode(node, methodEdges, messageList);
        Assertions.assertNull(methodEdges);
        Assertions.assertEquals(0, messageList.size());
    }

    @Test
    public void testCheckJoinNodeWithOnePreNodeUnDisabled(){
        List<Edge> edges = new ArrayList<>();
        List<Message> messageList = new ArrayList<>();
        List<Node> preNodes = new ArrayList<>();
        JoinProcessorNode node = getJoinNode(edges, preNodes);

        Node leftNode = preNodes.get(0);
        String leftNodeId = leftNode.getId();
        node.setLeftNodeId(leftNodeId);
        node.setRightNodeId(null);
        List<Edge> methodEdges = new ArrayList<>();
        methodEdges.add(edges.get(0));

        DAGCheckUtil.checkJoinNode(node, methodEdges, messageList);

        Assertions.assertEquals(1, messageList.size());
        Assertions.assertTrue(node.disabledNode());
    }

    @Test
    public void testGetTargetNodeSingleTarget() {
        Graph<Node, Edge> graph = new Graph<>();
        JsProcessorNode node1 = new JsProcessorNode();
        node1.setId("n1");
        JsProcessorNode node2 = new JsProcessorNode();
        node2.setId("n2");
        graph.setNode(node1.getId(), node1);
        graph.setNode(node2.getId(), node2);
        graph.setEdge(node1.getId(), node2.getId());
        DAG dag = new DAG(graph);
        node1.setDag(dag);
        node2.setDag(dag);
        node1.setGraph(graph);
        node2.setGraph(graph);

        Node<?> targetNode = DAGCheckUtil.getTargetNode(node1);
        Assertions.assertNotNull(targetNode);
        Assertions.assertEquals("n2", targetNode.getId());
    }

    @Test
    public void testGetTargetNodePreferDataNode() {
        Graph<Node, Edge> graph = new Graph<>();
        JsProcessorNode source = new JsProcessorNode();
        source.setId("s");
        DatabaseNode target = new DatabaseNode();
        target.setId("t");
        VirtualTargetNode virtualTarget = new VirtualTargetNode();
        virtualTarget.setId("v");
        graph.setNode(source.getId(), source);
        graph.setNode(target.getId(), target);
        graph.setNode(virtualTarget.getId(), virtualTarget);
        graph.setEdge(source.getId(), target.getId());
        graph.setEdge(source.getId(), virtualTarget.getId());
        DAG dag = new DAG(graph);
        source.setDag(dag);
        target.setDag(dag);
        virtualTarget.setDag(dag);
        source.setGraph(graph);
        target.setGraph(graph);
        virtualTarget.setGraph(graph);

        Node<?> targetNode = DAGCheckUtil.getTargetNode(source);
        Assertions.assertNotNull(targetNode);
        Assertions.assertEquals("t", targetNode.getId());
    }

    private JoinProcessorNode getJoinNode(List<Edge> edges, List<Node> preNodes) {
        JoinProcessorNode node = new JoinProcessorNode();
        final String joinId = "67yr4783yr";
        final String joinName = "JoinNode";
        final String leftId = "5783hff208585";
        final String rightId = "5783hff208586";
        final Node<?> nodeLeft = new JsProcessorNode();
        nodeLeft.setName("Js-left");
        nodeLeft.setId(leftId);
        final Node<?> nodeRight = new JsProcessorNode();
        nodeRight.setName("Js-right");
        nodeRight.setId(rightId);
        Graph<Node, Edge> graph = new Graph<>();
        graph.setNode(leftId, nodeLeft);
        graph.setNode(rightId, nodeRight);

        final Edge e2 = new Edge();
        e2.setSource(rightId);
        e2.setTarget(joinId);
        final io.github.openlg.graphlib.Edge e22 = new io.github.openlg.graphlib.Edge(rightId, joinId);
        graph.setEdge(e22);
        final Edge e1 = new Edge();
        e1.setSource(leftId);
        e1.setTarget(joinId);
        final io.github.openlg.graphlib.Edge e11 = new io.github.openlg.graphlib.Edge(leftId, joinId);
        graph.setEdge(e11);
        DAG dag = new DAG(graph);
        node.setDag(dag);

        node.setId(joinId);
        node.setName(joinName);

        edges.add(e1);
        edges.add(e2);
        preNodes.add(nodeLeft);
        preNodes.add(nodeRight);
        return node;
    }
}
