package com.tapdata.tm.dag.check;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.check.DAGCheckUtil;
import com.tapdata.tm.commons.dag.check.DAGNodeCheckItem;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.task.dto.Message;
import io.github.openlg.graphlib.Graph;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CheckDAGTest {

    /**
     * Test DAGCheckUtil.messageToList(List<Message> messageList, DAGNodeCheckItem code, String message)
     * */
    @Test
    public void checkDAGCheckUtilOfMessageToListMethod() {
        //case1 Using an null list to collect message, not throw exception
        DAGNodeCheckItem code = DAGNodeCheckItem.CHECK_JOIN_NODE;
        String message = "message";
        List<Message> list = null;
        try {
            DAGCheckUtil.messageToList(list, code, message);
        } catch (Exception e) {
            Assert.assertTrue("Using an null list for message collection, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }

        //case2 Using a list to collect message when code is null, return correct results
        list = new ArrayList<>();
        try {
            DAGCheckUtil.messageToList(list, null, message);
        } catch (Exception e) {
            Assert.assertTrue("Using a list for message collection and collect an null code, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }

        //case3 Using an empty list to collect message, return correct results
        list = new ArrayList<>();
        try {
            DAGCheckUtil.messageToList(list, code, message);
            Assert.assertTrue("Using an empty list to collect message, but size not meeting expectations", list.size() == 1);
            Assert.assertTrue(
                    "Using an empty list to collect message, but the code value of first element not meeting expectations",
                    code.getCode().equals(list.get(0).getCode()));
            Assert.assertTrue(
                    "Using an empty list to collect message, but the message value of first element not meeting expectations",
                    message.equals(list.get(0).getMsg()));
        } catch (Exception e) {
            Assert.assertTrue("Using a list for message collection, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }


        //case4 Using a list which contains some item to collect message, return correct results
        list = new ArrayList<>();
        Message m1 = new Message();
        m1.setCode("CODE");
        m1.setMsg("msg");
        list.add(m1);
        try {
            DAGCheckUtil.messageToList(list, code, message);
            Assert.assertTrue(
                    "Using a list which contains some item to collect message, but size not meeting expectations",
                    list.size() == 2);

            Assert.assertTrue(
                    "Using a list which contains some item to collect message, but the code value of first element not meeting expectations",
                    "CODE".equals(list.get(0).getCode()));
            Assert.assertTrue(
                    "Using a list which contains some item to collect message, but the message value of first element not meeting expectations",
                    "msg".equals(list.get(0).getMsg()));

            Assert.assertTrue(
                    "Using a list which contains some item to collect message, but the code value of last element not meeting expectations",
                    code.getCode().equals(list.get(1).getCode()));
            Assert.assertTrue(
                    "Using a list which contains some item to collect message, but the message value of last element not meeting expectations",
                    message.equals(list.get(1).getMsg()));
        } catch (Exception e) {
            Assert.assertTrue("Using a list which contains some item to collect message, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }
    }

    /**
     * Test DAGCheckUtil.setNodeToDisabled(Node<?> node)
     * */
    @Test
    public void checkDAGCheckUtilOfSetNodeToDisabledMethod(){
        //case 1: with null node
        Node<?> node = null;
        try {
            DAGCheckUtil.setNodeToDisabled(node);
        } catch (Exception e) {
            Assert.assertTrue("Using an null node to set disabled value as true, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }

        //case 2; with normal node, but attr map is null
        node = new JoinProcessorNode();
        node.setAttrs(null);
        try {
            DAGCheckUtil.setNodeToDisabled(node);
            Assert.assertTrue("Using an node to set disabled value as true but attr map is null, but node's disabled value not true", node.isDisabled());
            Assert.assertTrue("Using an node to set disabled value as true but attr map is null, but node's attr map is empty", null != node.getAttrs());
            Assert.assertTrue("Using an node to set disabled value as true but attr map is null, but node's attr not contains key 'disabled'", node.getAttrs().containsKey("disabled"));
            Assert.assertTrue(
                    "Using an node to set disabled value as true but attr map is null, node's attr contains key 'disabled', but value not true",
                     Boolean.TRUE.equals(node.getAttrs().get("disabled")));
        } catch (Exception e) {
            Assert.assertTrue("Using an node to set disabled value as true but attr map is null, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }

        //case 3; with normal node, but attr map is empty
        node = new JoinProcessorNode();
        node.setAttrs(new HashMap<>());
        try {
            DAGCheckUtil.setNodeToDisabled(node);
            Assert.assertTrue("Using an node to set disabled value as true but attr map is empty, but node's disabled value not true", node.isDisabled());
            Assert.assertTrue("Using an node to set disabled value as true but attr map is empty, but node's attr map is empty", null != node.getAttrs());
            Assert.assertTrue("Using an node to set disabled value as true but attr map is empty, but node's attr not contains key 'disabled'", node.getAttrs().containsKey("disabled"));
            Assert.assertTrue(
                    "Using an node to set disabled value as true but attr map is empty, node's attr contains key 'disabled', but value not true",
                    Boolean.TRUE.equals(node.getAttrs().get("disabled")));
        } catch (Exception e) {
            Assert.assertTrue("Using an node to set disabled value as true but attr map is empty, except no errors, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }

        //case 4; with normal node, but attr map contains 'disabled' key
        node = new JoinProcessorNode();
        Map<String, Object> attr = new HashMap<>();
        attr.put("disabled", false);
        node.setAttrs(attr);
        try {
            DAGCheckUtil.setNodeToDisabled(node);
            Assert.assertTrue("Using an node to set disabled value as true but attr map contains 'disabled' key, but node's disabled value not true", node.isDisabled());
            Assert.assertTrue("Using an node to set disabled value as true but attr map contains 'disabled' key, but node's attr map is empty", null != node.getAttrs());
            Assert.assertTrue("Using an node to set disabled value as true but attr map contains 'disabled' key, but node's attr not contains key 'disabled'", node.getAttrs().containsKey("disabled"));
            Assert.assertTrue(
                    "Using an node to set disabled value as true but attr map contains 'disabled' key, node's attr contains key 'disabled', but value not true",
                    Boolean.TRUE.equals(node.getAttrs().get("disabled")));
        } catch (Exception e) {
            Assert.assertTrue("Using an node to set disabled value as true but attr map contains 'disabled' key, expect no errors, but in reality, an error occurred: , expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }
    }

    /**
     * Test DAGCheckUtil.checkJoinNode(JoinProcessorNode joinNode, List<Edge> edges, List<Message> messageList)
     * */
    @Test
    public void checkDAGCheckUtilOfCheckJoinNodeMethod(){
        // case 1: null JoinProcessorNode
        List<Edge> edges = new ArrayList<>();
        List<Message> messageList = new ArrayList<>();
        try {
            DAGCheckUtil.checkJoinNode(null, edges, messageList);
        } catch (Exception e) {
            Assert.assertTrue("Using an null JoinProcessorNode to check, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }

        //case 2: disabled JoinProcessorNode
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



        node.setDisabled(true);
        Map<String, Object> attr = new HashMap<>();
        attr.put("disabled", true);
        node.setAttrs(attr);
        try {
            DAGCheckUtil.checkJoinNode(node, edges, messageList);
        } catch (Exception e) {
            Assert.assertTrue("Using an null JoinProcessorNode to check, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }

        //case 3: null edges list
        node.setDisabled(false);
        node.setAttrs(new HashMap<>());
        try {
            DAGCheckUtil.checkJoinNode(node, null, messageList);
        } catch (Exception e) {
            Assert.assertTrue("Using an null JoinProcessorNode to check, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }

        //case 4: empty edges list
        try {
            DAGCheckUtil.checkJoinNode(node, edges, messageList);
        } catch (Exception e) {
            Assert.assertTrue("Using an null JoinProcessorNode to check, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }

        //case 5: null message list
        try {
            DAGCheckUtil.checkJoinNode(node, edges, null);
        } catch (Exception e) {
            Assert.assertTrue("Using an null JoinProcessorNode to check, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }

        //case 6: join node not any source node
        try {
            DAGCheckUtil.checkJoinNode(node, edges, messageList);
            Assert.assertTrue(
                    "Using an node to set disabled value as true but attr map is null, but node's disabled value not true",
                    node.disabledNode());
        } catch (Exception e) {
            Assert.assertTrue("Using an null JoinProcessorNode to check, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }


        //case 7: join node only with one left source node
        node.setLeftNodeId(leftId);
        node.setRightNodeId(null);
        edges = new ArrayList<>();
        edges.add(e1);
        try {
            DAGCheckUtil.checkJoinNode(node, edges, messageList);
            Assert.assertTrue(
                    "Using an node to set disabled value as true but attr map is null, but node's disabled value not true",
                    node.disabledNode());
        } catch (Exception e) {
            Assert.assertTrue("Using an null JoinProcessorNode to check, expect no errors, but in reality, an error occurred: " + e.getMessage(), false);
        }


        //case 8: join node only with one right source node
        node.setLeftNodeId(null);
        node.setRightNodeId(rightId);
        edges = new ArrayList<>();
        edges.add(e2);


        //case 9: join node with one source node but source node has disabled
        node.setLeftNodeId(leftId);
        node.setRightNodeId(null);
        e2.setSource(rightId);
        e2.setTarget(joinId);
        edges = new ArrayList<>();
        edges.add(e2);
        nodeRight.setDisabled(true);
        nodeRight.setAttrs(new HashMap<String, Object>(){{put("disabled", true);}});


        //case 9: join node with two source nodes and any source node not disabled
        edges = new ArrayList<>();
        edges.add(e1);
        edges.add(e2);
        nodeRight.setDisabled(false);
        nodeRight.setAttrs(new HashMap<>());
        nodeLeft.setDisabled(false);
        nodeLeft.setAttrs(new HashMap<>());

    }
}
