package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import io.github.openlg.graphlib.Graph;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class DatabaseNodeTest {
    DatabaseNode databaseNode;

    @BeforeEach
    void init() {
        databaseNode = mock(DatabaseNode.class);
    }
    @Nested
    class transformSchemaTest{
        @DisplayName("test transformSchema main process ")
        @Test
        void test(){
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode,"tableNames",new ArrayList<>());
            ReflectionTestUtils.setField(databaseNode,"service",dagDataService);
            DAG.Options options = new DAG.Options();
            options.setBatchNum(1);
            doCallRealMethod().when(databaseNode).transformSchema(options);
            List<String> tableNames = new ArrayList<>();
            tableNames.add("test1");
            Graph graph = mock(Graph.class);
            when(databaseNode.getGraph()).thenReturn(graph);
            when(graph.predecessors(any())).thenReturn(new ArrayList<>());
            when(databaseNode.getSourceNodeTableNames(any())).thenReturn(tableNames);
            databaseNode.transformSchema(options);
            verify(dagDataService,times(0)).initializeModel(any());

        }
        @DisplayName("test transformSchema initializeModel ")
        @Test
        void test1(){
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode,"tableNames",new ArrayList<>());
            ReflectionTestUtils.setField(databaseNode,"service",dagDataService);
            DAG.Options options = new DAG.Options();
            options.setBatchNum(1);
            doCallRealMethod().when(databaseNode).transformSchema(options);
            List<String> tableNames = new ArrayList<>();
            tableNames.add("test1");
            Graph graph = mock(Graph.class);
            when(databaseNode.getGraph()).thenReturn(graph);
            when(graph.predecessors(any())).thenReturn(new ArrayList<>());
            when(databaseNode.getSourceNodeTableNames(any())).thenReturn(tableNames);
            when(dagDataService.whetherEngineDeduction()).thenReturn(true);
            databaseNode.transformSchema(options);
            verify(dagDataService,times(1)).initializeModel(any());

        }
        @DisplayName("test transformSchema initializeModel batch ")
        @Test
        void test2(){
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode,"tableNames",new ArrayList<>());
            ReflectionTestUtils.setField(databaseNode,"service",dagDataService);
            DAG.Options options = new DAG.Options();
            options.setBatchNum(1);
            doCallRealMethod().when(databaseNode).transformSchema(options);
            List<String> tableNames = new ArrayList<>();
            tableNames.add("test1");
            tableNames.add("test2");
            Graph graph = mock(Graph.class);
            when(databaseNode.getGraph()).thenReturn(graph);
            when(graph.predecessors(any())).thenReturn(new ArrayList<>());
            when(databaseNode.getSourceNodeTableNames(any())).thenReturn(tableNames);
            when(dagDataService.whetherEngineDeduction()).thenReturn(true);
            doAnswer(invocationOnMock -> {
                Boolean isLastBatch = invocationOnMock.getArgument(0);
                Assertions.assertFalse(isLastBatch);
                return null;
            }).doAnswer(invocationOnMock -> {
                Boolean isLastBatch = invocationOnMock.getArgument(0);
                Assertions.assertTrue(isLastBatch);
                return null;
            }).when(dagDataService).initializeModel(any());
            databaseNode.transformSchema(options);
            verify(dagDataService,times(2)).initializeModel(any());

        }
    }
}
