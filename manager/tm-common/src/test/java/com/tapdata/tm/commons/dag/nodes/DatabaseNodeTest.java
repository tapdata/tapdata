package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MigrateUnionProcessorNode;
import io.github.openlg.graphlib.Graph;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

public class DatabaseNodeTest {
    DatabaseNode databaseNode;

    @BeforeEach
    void init() {
        databaseNode = mock(DatabaseNode.class);
    }
    @Nested
    class transformSchemaTest {
        @DisplayName("test transformSchema main process ")
        @Test
        void test() {
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode, "tableNames", new ArrayList<>());
            ReflectionTestUtils.setField(databaseNode, "service", dagDataService);
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
            verify(dagDataService, times(0)).initializeModel(false);

        }

        @DisplayName("test transformSchema initializeModel ")
        @Test
        void test1() {
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode, "tableNames", new ArrayList<>());
            ReflectionTestUtils.setField(databaseNode, "service", dagDataService);
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
            verify(dagDataService, times(1)).initializeModel(false);

        }
    }

    @Nested
    class LoadSchemaTest {
        @Test
        void test_hasMigrateUnionNode(){
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode, "service", dagDataService);
            List<String> includes = new ArrayList<>();
            includes.add("test");
            when(databaseNode.getSyncType()).thenReturn("migrate");
            LinkedList<Node<?>> nodes = new LinkedList<>();
            nodes.add(new MigrateUnionProcessorNode());
            when(databaseNode.getPreNodes(any())).thenReturn(nodes);
            doCallRealMethod().when(databaseNode).loadSchema(includes);
            Assertions.assertNull(databaseNode.loadSchema(includes));
        }
        @Test
        void test_main(){
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode, "service", dagDataService);
            List<String> includes = new ArrayList<>();
            includes.add("test");
            when(databaseNode.getSyncType()).thenReturn("migrate");
            when(dagDataService.loadSchema(any(),any(),any(),any())).thenReturn(new ArrayList<>());
            doCallRealMethod().when(databaseNode).loadSchema(includes);
            Assertions.assertEquals(0,databaseNode.loadSchema(includes).size());
        }
    }
}
