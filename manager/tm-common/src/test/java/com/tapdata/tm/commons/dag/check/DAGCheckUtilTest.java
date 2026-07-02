package com.tapdata.tm.commons.dag.check;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.logCollector.VirtualTargetNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.task.dto.Message;
import io.github.openlg.graphlib.Graph;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
    public void testCheckJoinNodeWithMissingSourceNode(){
        List<Edge> edges = new ArrayList<>();
        List<Message> messageList = new ArrayList<>();
        List<Node> preNodes = new ArrayList<>();
        JoinProcessorNode node = getJoinNode(edges, preNodes);
        Node leftNode = preNodes.get(0);
        node.setLeftNodeId(leftNode.getId());
        node.setRightNodeId("missing-source");
        edges = new ArrayList<>();
        edges.add(new Edge(leftNode.getId(), node.getId()));
        edges.add(new Edge("missing-source", node.getId()));

        DAGCheckUtil.checkJoinNode(node, edges, messageList);

        Assertions.assertEquals(1, messageList.size());
        Assertions.assertEquals("DAG.JonNode", messageList.get(0).getCode());
        Assertions.assertTrue(messageList.get(0).getMsg().contains("missing-source"));
        Assertions.assertTrue(node.disabledNode());
    }

    @Test
    public void testCheckJoinNodeWithDisabledSourceNode(){
        List<Edge> edges = new ArrayList<>();
        List<Message> messageList = new ArrayList<>();
        List<Node> preNodes = new ArrayList<>();
        JoinProcessorNode node = getJoinNode(edges, preNodes);
        Node leftNode = preNodes.get(0);
        Node rightNode = preNodes.get(1);
        node.setLeftNodeId(leftNode.getId());
        node.setRightNodeId(rightNode.getId());
        rightNode.setDisabled(true);
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("disabled", true);
        rightNode.setAttrs(attrs);

        DAGCheckUtil.checkJoinNode(node, edges, messageList);

        Assertions.assertEquals(1, messageList.size());
        Assertions.assertEquals("DAG.JonNode", messageList.get(0).getCode());
        Assertions.assertTrue(messageList.get(0).getMsg().contains(rightNode.getName()));
        Assertions.assertTrue(node.disabledNode());
    }

    @Test
    public void testGetTargetNodeNullNode() {
        Assertions.assertNull(DAGCheckUtil.getTargetNode(null));
    }

    @Test
    public void testGetTargetNodeNullDag() {
        JsProcessorNode node = node("n1");

        Assertions.assertNull(DAGCheckUtil.getTargetNode(node));
    }

    @Test
    public void testGetTargetNodeEmptyTargets() {
        JsProcessorNode node = node("n1");
        DAG dag = new DAG(new Graph<>());
        node.setDag(dag);

        Assertions.assertNull(DAGCheckUtil.getTargetNode(node));
    }

    @Test
    public void testGetTargetNodeNullTargets() {
        JsProcessorNode node = node("n1");
        DAG dag = new StubDag(null);
        node.setDag(dag);

        Assertions.assertNull(DAGCheckUtil.getTargetNode(node));
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

    @Test
    public void testGetTargetNodeChooseDeepestNonVirtualTarget() {
        JsProcessorNode source = node("source");
        JsProcessorNode middle = node("middle");
        JsProcessorNode shallowTarget = node("target-z");
        JsProcessorNode deepTarget = node("target-y");
        Graph<Node, Edge> graph = graphWithNodes(source, middle, shallowTarget, deepTarget);
        graph.setEdge(source.getId(), shallowTarget.getId(), new Edge(source.getId(), shallowTarget.getId()));
        graph.setEdge(source.getId(), middle.getId(), new Edge(source.getId(), middle.getId()));
        graph.setEdge(middle.getId(), deepTarget.getId(), new Edge(middle.getId(), deepTarget.getId()));
        bindDag(graph, source, middle, shallowTarget, deepTarget);

        Node<?> targetNode = DAGCheckUtil.getTargetNode(source);

        Assertions.assertNotNull(targetNode);
        Assertions.assertEquals(deepTarget.getId(), targetNode.getId());
    }

    @Test
    public void testGetTargetNodeChooseLowerIdWhenDepthEqual() {
        JsProcessorNode source = node("source");
        JsProcessorNode targetB = node("target-b");
        JsProcessorNode targetA = node("target-a");
        Graph<Node, Edge> graph = graphWithNodes(source, targetB, targetA);
        graph.setEdge(source.getId(), targetB.getId(), new Edge(source.getId(), targetB.getId()));
        graph.setEdge(source.getId(), targetA.getId(), new Edge(source.getId(), targetA.getId()));
        bindDag(graph, source, targetB, targetA);

        Node<?> targetNode = DAGCheckUtil.getTargetNode(source);

        Assertions.assertNotNull(targetNode);
        Assertions.assertEquals(targetA.getId(), targetNode.getId());
    }

    @Test
    public void testGetTargetNodeFallbackToVirtualAndLogCollectorTargets() {
        JsProcessorNode source = node("source");
        JsProcessorNode middle = node("middle");
        VirtualTargetNode virtualTarget = new VirtualTargetNode();
        virtualTarget.setId("virtual-target");
        LogCollectorNode logCollectorNode = new LogCollectorNode();
        logCollectorNode.setId("log-target");
        Graph<Node, Edge> graph = graphWithNodes(source, middle, virtualTarget, logCollectorNode);
        graph.setEdge(source.getId(), virtualTarget.getId(), new Edge(source.getId(), virtualTarget.getId()));
        graph.setEdge(source.getId(), middle.getId(), new Edge(source.getId(), middle.getId()));
        graph.setEdge(middle.getId(), logCollectorNode.getId(), new Edge(middle.getId(), logCollectorNode.getId()));
        bindDag(graph, source, middle, virtualTarget, logCollectorNode);

        Node<?> targetNode = DAGCheckUtil.getTargetNode(source);

        Assertions.assertNotNull(targetNode);
        Assertions.assertEquals(logCollectorNode.getId(), targetNode.getId());
    }

    @Test
    public void testGetTargetNodeIgnoreInvalidCandidates() {
        JsProcessorNode source = node("source");
        JsProcessorNode noIdTarget = new JsProcessorNode();
        JsProcessorNode candidate = node("candidate");
        StubDag dag = new StubDag(Arrays.asList(null, noIdTarget, candidate));
        source.setDag(dag);

        Node<?> targetNode = DAGCheckUtil.getTargetNode(source);

        Assertions.assertSame(candidate, targetNode);
    }

    @Test
    public void testGetTargetNodeHandleCycleAndInvalidPredecessor() {
        JsProcessorNode source = node("source");
        JsProcessorNode cycleTarget = node("cycle-target");
        JsProcessorNode flatTarget = node("flat-target");
        StubDag dag = new StubDag(Arrays.asList(cycleTarget, flatTarget));
        dag.withPredecessors(cycleTarget.getId(), Arrays.asList(null, cycleTarget));
        source.setDag(dag);

        Node<?> targetNode = DAGCheckUtil.getTargetNode(source);

        Assertions.assertSame(cycleTarget, targetNode);
    }

    @Test
    public void testPreNodeCountNullDag() {
        Assertions.assertEquals(0, DAGCheckUtil.preNodeCount(node("target")));
    }

    @Test
    public void testPreNodeCount() {
        JsProcessorNode source1 = node("source-1");
        JsProcessorNode source2 = node("source-2");
        JsProcessorNode target = node("target");
        JsProcessorNode next = node("next");
        Graph<Node, Edge> graph = graphWithNodes(source1, source2, target, next);
        graph.setEdge(source1.getId(), target.getId(), new Edge(source1.getId(), target.getId()));
        graph.setEdge(source2.getId(), target.getId(), new Edge(source2.getId(), target.getId()));
        graph.setEdge(target.getId(), next.getId(), new Edge(target.getId(), next.getId()));
        bindDag(graph, source1, source2, target, next);

        Assertions.assertEquals(2, DAGCheckUtil.preNodeCount(target));
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

    private JsProcessorNode node(String id) {
        JsProcessorNode node = new JsProcessorNode();
        node.setId(id);
        return node;
    }

    private Graph<Node, Edge> graphWithNodes(Node<?>... nodes) {
        Graph<Node, Edge> graph = new Graph<>();
        for (Node<?> node : nodes) {
            graph.setNode(node.getId(), node);
        }
        return graph;
    }

    private DAG bindDag(Graph<Node, Edge> graph, Node<?>... nodes) {
        DAG dag = new DAG(graph);
        for (Node<?> node : nodes) {
            node.setDag(dag);
            node.setGraph(graph);
        }
        return dag;
    }

    private static class StubDag extends DAG {
        private final List<Node> targets;
        private final Map<String, List<Node>> predecessors = new HashMap<>();

        private StubDag(List<Node> targets) {
            super(new Graph<>());
            this.targets = targets;
        }

        private StubDag withPredecessors(String id, List<Node> predecessors) {
            this.predecessors.put(id, predecessors);
            return this;
        }

        @Override
        public List<Node> getTargets() {
            return targets;
        }

        @Override
        public List<Node> predecessors(String id) {
            return predecessors.getOrDefault(id, new ArrayList<>());
        }
    }
}
