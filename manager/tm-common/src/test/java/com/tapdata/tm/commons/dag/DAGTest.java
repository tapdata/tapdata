package com.tapdata.tm.commons.dag;

import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class DAGTest {
    DAG dag;
    @BeforeEach
    void init() {
        dag = mock(DAG.class);
    }

    @Nested
    class GetTaskDtoIsomorphismTest {
        List<Node> nodeList;

        @BeforeEach
        void init() {
            nodeList = mock(ArrayList.class);
            when(nodeList.size()).thenReturn(2);

            when(dag.getTaskDtoIsomorphism(anyList())).thenCallRealMethod();
            when(dag.getTaskDtoIsomorphism(null)).thenCallRealMethod();
        }

        @Test
        void testGetTaskDtoIsomorphismNormal() {
            DataParentNode node1 = mock(DataParentNode.class);
            when(node1.getDatabaseType()).thenReturn("mock-type");
            when(nodeList.get(0)).thenReturn(node1);

            DataParentNode node2 = mock(DataParentNode.class);
            when(node2.getDatabaseType()).thenReturn("mock-type");
            when(nodeList.get(1)).thenReturn(node2);

            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertTrue(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(1)).get(0);
            verify(nodeList, times(1)).get(1);

            verify(node1, times(1)).getDatabaseType();
            verify(node2, times(1)).getDatabaseType();
        }

        @Test
        void testGetTaskDtoIsomorphismNullNodeList() {
            when(nodeList.size()).thenReturn(0);
            boolean isomorphism = dag.getTaskDtoIsomorphism(null);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(0)).size();
            verify(nodeList, times(0)).get(0);
            verify(nodeList, times(0)).get(1);
        }

        @Test
        void testGetTaskDtoIsomorphismEmptyNodeList() {
            when(nodeList.size()).thenReturn(0);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(0)).get(0);
            verify(nodeList, times(0)).get(1);
        }

        @Test
        void testGetTaskDtoIsomorphismMoreThanTwoNode() {
            when(nodeList.size()).thenReturn(100);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(0)).get(0);
            verify(nodeList, times(0)).get(1);
        }

        @Test
        void testGetTaskDtoIsomorphismLessThanTwoNode() {
            when(nodeList.size()).thenReturn(1);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(0)).get(0);
            verify(nodeList, times(0)).get(1);
        }

        @Test
        void testGetTaskDtoIsomorphismTwoNodeAndAllNodeAreDataParentNodeButDataTypeNotEquals() {
            when(nodeList.size()).thenReturn(2);
            DataParentNode node1 = mock(DataParentNode.class);
            when(node1.getDatabaseType()).thenReturn("mock-type-cache");
            when(nodeList.get(0)).thenReturn(node1);

            DataParentNode node2 = mock(DataParentNode.class);
            when(node2.getDatabaseType()).thenReturn("mock-type");
            when(nodeList.get(1)).thenReturn(node2);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(1)).get(0);
            verify(nodeList, times(1)).get(1);
            verify(node2, times(1)).getDatabaseType();
            verify(node2, times(1)).getDatabaseType();
        }

        @Test
        void testGetTaskDtoIsomorphismTwoNodeButNotAllNodeAreDataParentNode() {
            when(nodeList.size()).thenReturn(2);
            JsProcessorNode node1 = mock(JsProcessorNode.class);
            when(nodeList.get(0)).thenReturn(node1);

            DataParentNode node2 = mock(DataParentNode.class);
            when(node2.getDatabaseType()).thenReturn("mock-type");
            when(nodeList.get(1)).thenReturn(node2);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(1)).get(0);
            verify(nodeList, times(0)).get(1);
            verify(node2, times(0)).getDatabaseType();
        }

        @Test
        void testGetTaskDtoIsomorphismTwoNodeButNotAllNodeAreDataParentNode0() {
            when(nodeList.size()).thenReturn(2);
            JsProcessorNode node2 = mock(JsProcessorNode.class);
            when(nodeList.get(0)).thenReturn(node2);

            DataParentNode node1 = mock(DataParentNode.class);
            when(node1.getDatabaseType()).thenReturn("mock-type");
            when(nodeList.get(1)).thenReturn(node1);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(1)).get(0);
            verify(nodeList, times(0)).get(1);
            verify(node1, times(0)).getDatabaseType();
        }

        @Test
        void testGetTaskDtoIsomorphismTwoNodeButNotAllNodeAreDataParentNode1() {
            when(nodeList.size()).thenReturn(2);
            DataParentNode node2 = mock(DataParentNode.class);
            when(node2.getDatabaseType()).thenReturn("mock-type");
            when(nodeList.get(0)).thenReturn(node2);

            JsProcessorNode node1 = mock(JsProcessorNode.class);
            when(nodeList.get(1)).thenReturn(node1);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(1)).get(0);
            verify(nodeList, times(1)).get(1);
            verify(node2, times(0)).getDatabaseType();
        }

        @Test
        void testGetTaskDtoIsomorphismTwoNodeButAllNodeNotDataParentNode() {
            when(nodeList.size()).thenReturn(2);
            JsProcessorNode node1 = mock(JsProcessorNode.class);
            when(nodeList.get(0)).thenReturn(node1);

            JsProcessorNode node2 = mock(JsProcessorNode.class);
            when(nodeList.get(1)).thenReturn(node2);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(1)).get(0);
            verify(nodeList, times(0)).get(1);
        }

        @Test
        void testGetTaskDtoIsomorphismTwoNodeButAllNodeAreNull() {
            when(nodeList.size()).thenReturn(2);
            when(nodeList.get(0)).thenReturn(null);
            when(nodeList.get(1)).thenReturn(null);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(1)).get(0);
            verify(nodeList, times(0)).get(1);
        }

        @Test
        void testGetTaskDtoIsomorphismTwoNodeButNotAllNodeAreNull() {
            when(nodeList.size()).thenReturn(2);
            when(nodeList.get(0)).thenReturn(null);

            JsProcessorNode node2 = mock(JsProcessorNode.class);
            when(nodeList.get(1)).thenReturn(node2);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(1)).get(0);
            verify(nodeList, times(0)).get(1);
        }

        @Test
        void testGetTaskDtoIsomorphismTwoNodeButNotAllNodeAreNull1() {
            when(nodeList.size()).thenReturn(2);
            when(nodeList.get(1)).thenReturn(null);

            JsProcessorNode node2 = mock(JsProcessorNode.class);
            when(nodeList.get(0)).thenReturn(node2);
            boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
            Assertions.assertFalse(isomorphism);
            verify(nodeList, times(1)).size();
            verify(nodeList, times(1)).get(0);
            verify(nodeList, times(0)).get(1);
        }
    }

    @Nested
    class SetIsomorphismValueToOptionsTest {
        DAG.Options options;
        List<Node> nodeList;

        @BeforeEach
        void init() {
            options = mock(DAG.Options.class);
            nodeList = mock(ArrayList.class);
            doNothing().when(options).setIsomorphismTask(anyBoolean());
            when(dag.getTaskDtoIsomorphism(anyList())).thenReturn(true);

            doCallRealMethod().when(dag).setIsomorphismValueToOptions(any(DAG.Options.class), anyList());
            doCallRealMethod().when(dag).setIsomorphismValueToOptions(null, nodeList);
        }

        @Test
        void testSetIsomorphismValueToOptionsNormal() {
            dag.setIsomorphismValueToOptions(options, nodeList);
            verify(dag, times(1)).getTaskDtoIsomorphism(nodeList);
            verify(options, times(1)).setIsomorphismTask(anyBoolean());
        }

        @Test
        void testSetIsomorphismValueToOptionsWithNullOptions() {
            dag.setIsomorphismValueToOptions(null, nodeList);
            verify(dag, times(0)).getTaskDtoIsomorphism(nodeList);
            verify(options, times(0)).setIsomorphismTask(anyBoolean());
        }

    }
}