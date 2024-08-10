package io.tapdata.flow.engine.V2.node.hazelcast.controller;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import io.tapdata.flow.engine.V2.util.GraphUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.*;

import static org.mockito.Mockito.*;

public class SnapshotOrderControllerTest {

    @Test
    void test_recursiveBuildSnapshotOrderListByMergeNode(){
        MergeTableProperties mergeTableProperties = new MergeTableProperties();
        mergeTableProperties.setId("test");
        List<MergeTableProperties> mergeTablePropertiesList = Arrays.asList(mergeTableProperties);
        MergeTableNode mergeNode = mock(MergeTableNode.class);
        when(mergeNode.predecessors()).thenReturn(new ArrayList<>());
        try(MockedStatic<GraphUtil> graphUtilMockedStatic = mockStatic(GraphUtil.class)){
            List<Node<?>> sourceTableNodes = new ArrayList<>();
            TableNode tableNode = new TableNode();
            tableNode.setDisabled(false);
            sourceTableNodes.add(tableNode);

            graphUtilMockedStatic.when(() -> GraphUtil.predecessors(any(), any(),any())).thenReturn(sourceTableNodes);
            List<NodeControlLayer> nodeControlLayers = new ArrayList<>();
            SnapshotOrderController.recursiveBuildSnapshotOrderListByMergeNode(mergeTablePropertiesList,nodeControlLayers, mergeNode,0);
            Assertions.assertEquals(1,nodeControlLayers.size());
        }
    }
    @Test
    void test_recursiveBuildSnapshotOrderListByMergeNode_when_disabled(){
        MergeTableProperties mergeTableProperties = new MergeTableProperties();
        mergeTableProperties.setId("test");
        List<MergeTableProperties> mergeTablePropertiesList = Arrays.asList(mergeTableProperties);
        MergeTableNode mergeNode = mock(MergeTableNode.class);
        when(mergeNode.predecessors()).thenReturn(new ArrayList<>());
        try(MockedStatic<GraphUtil> graphUtilMockedStatic = mockStatic(GraphUtil.class)){
            List<Node<?>> sourceTableNodes = new ArrayList<>();
            TableNode tableNode = new TableNode();
            Map<String,Object> attr = new HashMap<>();
            attr.put("disabled",true);
            tableNode.setAttrs(attr);
            sourceTableNodes.add(tableNode);
            graphUtilMockedStatic.when(() -> GraphUtil.predecessors(any(), any(),any())).thenReturn(sourceTableNodes);
            List<NodeControlLayer> nodeControlLayers = new ArrayList<>();
            SnapshotOrderController.recursiveBuildSnapshotOrderListByMergeNode(mergeTablePropertiesList,nodeControlLayers, mergeNode,0);
            Assertions.assertEquals(0,nodeControlLayers.size());
        }
    }
}
