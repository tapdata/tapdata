package com.tapdata.tm.task.service.impl;


import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.PageParameter;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesServiceImpl;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskNodeServiceImplTest {
    TaskNodeServiceImpl taskNodeService;
    AgentGroupService agentGroupService;
    WorkerService workerService;
    MessageQueueService messageQueueService;

    UserDetail userDetail;
    MetadataInstancesService metadataInstancesService;

    @BeforeEach
    void init() {
        taskNodeService = mock(TaskNodeServiceImpl.class);
        agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(taskNodeService, "agentGroupService", agentGroupService);
        workerService = mock(WorkerService.class);
        ReflectionTestUtils.setField(taskNodeService, "workerService", workerService);
        messageQueueService = mock(MessageQueueService.class);
        ReflectionTestUtils.setField(taskNodeService, "messageQueueService", messageQueueService);
        ReflectionTestUtils.setField(taskNodeService, "dataSourceService", mock(DataSourceService.class));
        metadataInstancesService = mock(MetadataInstancesService.class);
        ReflectionTestUtils.setField(taskNodeService, "metadataInstancesService", metadataInstancesService);

        userDetail = mock(UserDetail.class);
    }

    @Nested
    class SendMessageAfterFindAgentTest {
        TaskDto taskDto;
        TaskDto taskDtoCopy;

        List<Worker> workers;
        Worker worker;
        List<String> nodeList;
        @BeforeEach
        void init() {
            taskDto = mock(TaskDto.class);
            taskDtoCopy = mock(TaskDto.class);

            nodeList = mock(List.class);
            workers = new ArrayList<>();
            worker = mock(Worker.class);
            when(worker.getProcessId()).thenReturn("id");
            workers.add(worker);

            when(agentGroupService.getProcessNodeListWithGroup(taskDto, userDetail)).thenReturn(nodeList);
            when(workerService.findAvailableAgentByAccessNode(userDetail, nodeList)).thenReturn(workers);
            doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
            doCallRealMethod().when(taskNodeService).sendMessageAfterFindAgent(taskDto, taskDtoCopy, userDetail);
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> taskNodeService.sendMessageAfterFindAgent(taskDto, taskDtoCopy, userDetail));
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, userDetail);
            verify(workerService, times(1)).findAvailableAgentByAccessNode(userDetail, nodeList);
            verify(worker, times(1)).getProcessId();
            verify(messageQueueService, times(1)).sendMessage(any(MessageQueueDto.class));
        }
        @Test
        void testNotAnyWorkers() {
            workers.remove(0);
            Assertions.assertThrows(BizException.class, () -> taskNodeService.sendMessageAfterFindAgent(taskDto, taskDtoCopy, userDetail));
            verify(agentGroupService, times(1)).getProcessNodeListWithGroup(taskDto, userDetail);
            verify(workerService, times(1)).findAvailableAgentByAccessNode(userDetail, nodeList);
            verify(worker, times(0)).getProcessId();
            verify(messageQueueService, times(0)).sendMessage(any(MessageQueueDto.class));
        }
    }

    @Nested
    class WsTestRunTest {
        @Test
        void testNormal() {
            doNothing().when(taskNodeService).sendMessageAfterFindAgent(any(TaskDto.class), any(TaskDto.class), any(UserDetail.class));
            doCallRealMethod().when(taskNodeService).wsTestRun(any(UserDetail.class), any(TaskDto.class), any(TaskDto.class));

            Map<String, Object> map = taskNodeService.wsTestRun(userDetail, mock(TaskDto.class), mock(TaskDto.class));
            Assertions.assertNotNull(map);
            Assertions.assertEquals(HashMap.class.getName(), map.getClass().getName());
            verify(taskNodeService, times(1)).sendMessageAfterFindAgent(any(TaskDto.class), any(TaskDto.class), any(UserDetail.class));
        }
    }
    @Nested
    class CheckMigrateTableSelectType{
        @Test
        void test_type_all(){
            doCallRealMethod().when(taskNodeService).getMigrateTableNames(any(),any());
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setMigrateTableSelectType("all");
            databaseNode.setTableNames(Arrays.asList("table1","table2"));
            List<String> result = taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            Assertions.assertEquals(2,result.size());
        }

        @Test
        void test_type_expression_main(){
            doCallRealMethod().when(taskNodeService).getMigrateTableNames(any(),any());
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setMigrateTableSelectType("expression");
            databaseNode.setTableExpression("test.*");
            taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setOriginalName("test1");
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            metadataInstancesDto2.setOriginalName("test2");
            MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
            when(metadataInstancesService.findSourceSchemaBySourceId(any(),any(),any(),any())).thenReturn(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3));
            List<String> result = taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            Assertions.assertEquals(2,result.size());
        }

        @Test
        void test_type_expression_tableExpression_isNull(){
            doCallRealMethod().when(taskNodeService).getMigrateTableNames(any(),any());
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setMigrateTableSelectType("expression");
            taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setOriginalName("test1");
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            metadataInstancesDto2.setOriginalName("test2");
            MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
            when(metadataInstancesService.findSourceSchemaBySourceId(any(),any(),any(),any())).thenReturn(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3));
            List<String> result = taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            Assertions.assertEquals(0,result.size());
        }

        @Test
        void test_type_expression_NoPrimaryKeyTableSelectType_is_HasKeys(){
            doCallRealMethod().when(taskNodeService).getMigrateTableNames(any(),any());
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setMigrateTableSelectType("expression");
            databaseNode.setTableExpression("test.*");
            databaseNode.setNoPrimaryKeyTableSelectType("HasKeys");
            taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setOriginalName("test1");
            Field field1 = new Field();
            field1.setPrimaryKey(true);
            metadataInstancesDto1.setFields(Arrays.asList(field1));
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            Field field2 = new Field();
            field2.setPrimaryKey(false);
            metadataInstancesDto2.setOriginalName("test2");
            metadataInstancesDto2.setFields(Arrays.asList(field2));
            MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
            when(metadataInstancesService.findSourceSchemaBySourceId(any(),any(),any(),any())).thenReturn(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3));
            List<String> result = taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            Assertions.assertEquals(1,result.size());
        }

        @Test
        void test_type_expression_NoPrimaryKeyTableSelectType_is_NoKeys(){
            doCallRealMethod().when(taskNodeService).getMigrateTableNames(any(),any());
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setMigrateTableSelectType("expression");
            databaseNode.setTableExpression("test.*");
            databaseNode.setNoPrimaryKeyTableSelectType("NoKeys");
            taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setOriginalName("test1");
            Field field1 = new Field();
            field1.setPrimaryKey(true);
            metadataInstancesDto1.setFields(Arrays.asList(field1));
            TableIndex index1 = new TableIndex();
            index1.setUnique(true);
            metadataInstancesDto1.setIndices(Arrays.asList(index1));
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            Field field2 = new Field();
            field2.setPrimaryKey(false);
            metadataInstancesDto2.setOriginalName("test2");
            metadataInstancesDto2.setFields(Arrays.asList(field2));
            TableIndex index2 = new TableIndex();
            index2.setUnique(false);
            metadataInstancesDto2.setIndices(Arrays.asList(index2));
            MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
            when(metadataInstancesService.findSourceSchemaBySourceId(any(),any(),any(),any())).thenReturn(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3));
            List<String> result = taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            Assertions.assertEquals(1,result.size());
        }
        @Test
        void test_type_expression_NoPrimaryKeyTableSelectType_is_All(){
            doCallRealMethod().when(taskNodeService).getMigrateTableNames(any(),any());
            DatabaseNode databaseNode = new DatabaseNode();
            databaseNode.setMigrateTableSelectType("expression");
            databaseNode.setTableExpression("test.*");
            databaseNode.setNoPrimaryKeyTableSelectType("All");
            taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
            metadataInstancesDto1.setOriginalName("test1");
            Field field1 = new Field();
            field1.setPrimaryKey(true);
            metadataInstancesDto1.setFields(Arrays.asList(field1));
            MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
            Field field2 = new Field();
            field2.setPrimaryKey(false);
            metadataInstancesDto2.setOriginalName("test2");
            metadataInstancesDto2.setFields(Arrays.asList(field2));
            MetadataInstancesDto metadataInstancesDto3 = new MetadataInstancesDto();
            when(metadataInstancesService.findSourceSchemaBySourceId(any(),any(),any(),any())).thenReturn(Arrays.asList(metadataInstancesDto1,metadataInstancesDto2,metadataInstancesDto3));
            List<String> result = taskNodeService.getMigrateTableNames(databaseNode,mock(UserDetail.class));
            Assertions.assertEquals(2,result.size());
        }

    }
    @Nested
    class CheckUnionProcess{
        @Test
        void test_main(){
            DAG dag = mock(DAG.class);
            LinkedList<Node<?>> preNodes = new LinkedList<>();
            preNodes.add(new MigrateDateProcessorNode());
            when(dag.getPreNodes(any())).thenReturn(preNodes);
            doCallRealMethod().when(taskNodeService).checkUnionProcess(any(),any(),any());
            List<String> exceptTableNames = new ArrayList<>();
            taskNodeService.checkUnionProcess(dag,"nodeId",exceptTableNames);
            Assertions.assertEquals(0,exceptTableNames.size());
        }
        @Test
        void test_hashUnionNode(){
            DAG dag = mock(DAG.class);
            LinkedList<Node<?>> preNodes = new LinkedList<>();
            MigrateUnionProcessorNode migrateUnionProcessorNode = new MigrateUnionProcessorNode();
            migrateUnionProcessorNode.setTableName("union_test");
            preNodes.add(migrateUnionProcessorNode);
            when(dag.getPreNodes(any())).thenReturn(preNodes);
            doCallRealMethod().when(taskNodeService).checkUnionProcess(any(),any(),any());
            List<String> exceptTableNames = new ArrayList<>();
            taskNodeService.checkUnionProcess(dag,"nodeId",exceptTableNames);
            Assertions.assertEquals("union_test",exceptTableNames.get(0));
        }
        @Test
        void test_jsNode(){
            DAG dag = mock(DAG.class);
            LinkedList<Node<?>> preNodes = new LinkedList<>();
            preNodes.add(new MigrateUnionProcessorNode());
            when(dag.getPreNodes(any())).thenReturn(preNodes);
            when(dag.getNode(any())).thenReturn((Node)new StandardMigrateJsProcessorNode());
            doCallRealMethod().when(taskNodeService).checkUnionProcess(any(),any(),any());
            List<String> exceptTableNames = new ArrayList<>();
            taskNodeService.checkUnionProcess(dag,"nodeId",exceptTableNames);
            Assertions.assertEquals(0,exceptTableNames.size());
        }
    }
    @Nested
    class GetNodeIdIfContainsMigrateUnionNodeTest {
        DatabaseNode sourceNode;
        DAG dag;
        List<Node> nodes;
        Node node1;
        Node node2;
        @BeforeEach
        void init() {
            sourceNode = mock(DatabaseNode.class);
            dag = mock(DAG.class);
            nodes = new ArrayList<>();
            node1 = mock(Node.class);
            node2 = mock(Node.class);
            nodes.add(node1);
            nodes.add(null);
            nodes.add(node2);
            when(node1.getType()).thenReturn("type");
            when(node2.getType()).thenReturn(NodeEnum.migrate_union_processor.name());
            when(sourceNode.getDag()).thenReturn(dag);
            when(sourceNode.getId()).thenReturn("id");
            when(dag.getNodes()).thenReturn(nodes);
            when(node1.getId()).thenReturn("id");
            when(node2.getId()).thenReturn("id");
            when(taskNodeService.getNodeIdIfContainsMigrateUnionNode(any(DatabaseNode.class))).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertNotNull(taskNodeService.getNodeIdIfContainsMigrateUnionNode(sourceNode));
            verify(node1, times(1)).getType();
            verify(node2, times(1)).getType();
            verify(sourceNode, times(1)).getDag();
            verify(sourceNode, times(0)).getId();
            verify(node1, times(0)).getId();
            verify(node2, times(1)).getId();
            verify(dag, times(1)).getNodes();
        }
        @Test
        void testDAGIsNull() {
            when(sourceNode.getDag()).thenReturn(null);
            Assertions.assertNotNull(taskNodeService.getNodeIdIfContainsMigrateUnionNode(sourceNode));
            verify(node1, times(0)).getType();
            verify(node2, times(0)).getType();
            verify(sourceNode, times(1)).getDag();
            verify(sourceNode, times(1)).getId();
            verify(node1, times(0)).getId();
            verify(node2, times(0)).getId();
            verify(dag, times(0)).getNodes();
        }
        @Test
        void testNodesIsEmpty() {
            nodes.clear();
            Assertions.assertNotNull(taskNodeService.getNodeIdIfContainsMigrateUnionNode(sourceNode));
            verify(node1, times(0)).getType();
            verify(node2, times(0)).getType();
            verify(sourceNode, times(1)).getDag();
            verify(sourceNode, times(1)).getId();
            verify(node1, times(0)).getId();
            verify(node2, times(0)).getId();
            verify(dag, times(1)).getNodes();
        }
        @Test
        void testNotContainsUnionNode() {
            nodes.clear();
            nodes.add(node1);
            Assertions.assertNotNull(taskNodeService.getNodeIdIfContainsMigrateUnionNode(sourceNode));
            verify(node1, times(1)).getType();
            verify(node2, times(0)).getType();
            verify(sourceNode, times(1)).getDag();
            verify(sourceNode, times(1)).getId();
            verify(node1, times(0)).getId();
            verify(node2, times(0)).getId();
            verify(dag, times(1)).getNodes();
        }
    }

    @Nested
    class GetNodeInfoByMigrateTest {
        private DAG dag;
        private DatabaseNode sourceNode;
        private DatabaseNode targetNode;
        private DataSourceService dataSourceService;
        private DataSourceConnectionDto sourceDataSource;
        private DataSourceConnectionDto targetDataSource;
        private TableRenameProcessNode tableRenameProcessNode;

        @BeforeEach
        void setUp() {
            dag = mock(DAG.class);
            sourceNode = mock(DatabaseNode.class);
            targetNode = mock(DatabaseNode.class);
            dataSourceService = mock(DataSourceService.class);
            sourceDataSource = mock(DataSourceConnectionDto.class);
            targetDataSource = mock(DataSourceConnectionDto.class);
            tableRenameProcessNode = mock(TableRenameProcessNode.class);

            ReflectionTestUtils.setField(taskNodeService, "dataSourceService", dataSourceService);

            // Mock basic setup
            when(dag.getSourceNode("nodeId")).thenReturn(sourceNode);
            LinkedList<DatabaseNode> targetNodes = new LinkedList<>();
            targetNodes.add(targetNode);
            when(dag.getTargetNode()).thenReturn(targetNodes);
            when(dag.getTargetNode("nodeId")).thenReturn(targetNode);
            when(sourceNode.getConnectionId()).thenReturn("sourceConnectionId");
            when(targetNode.getConnectionId()).thenReturn("targetConnectionId");
            when(dataSourceService.findById(any(ObjectId.class))).thenReturn(targetDataSource);
            LinkedList<Node<?>> preNodes = new LinkedList<>();
            preNodes.add(tableRenameProcessNode);
            when(dag.getPreNodes("nodeId")).thenReturn(preNodes);
            when(dag.nodeMap()).thenReturn(new HashMap<>());
            when(dag.getNode("nodeId")).thenReturn((Node)sourceNode);

            // Mock getMigrateTableNames method
            doCallRealMethod().when(taskNodeService).getNodeInfoByMigrate(
                any(String.class), any(String.class), any(String.class),
                any(PageParameter.class), any(UserDetail.class),
                any(Page.class), any(DAG.class));
        }

        @Test
        void testGetNodeInfoByMigrate_SourceNodeIsNull() {
            when(dag.getSourceNode("nodeId")).thenReturn(null);
            Page<MetadataTransformerItemDto> result = new Page<>();

            Page<MetadataTransformerItemDto> actualResult = taskNodeService.getNodeInfoByMigrate(
                "taskId", "nodeId", "searchTable",
                new PageParameter(1, 10), userDetail, result, dag);

            Assertions.assertEquals(result, actualResult);
            verify(dag, times(1)).getSourceNode("nodeId");
        }

        @Test
        void testGetNodeInfoByMigrate_EmptyTableNames() {
            when(taskNodeService.getMigrateTableNames(sourceNode, userDetail)).thenReturn(new ArrayList<>());
            Page<MetadataTransformerItemDto> result = new Page<>();

            Page<MetadataTransformerItemDto> actualResult = taskNodeService.getNodeInfoByMigrate(
                "taskId", "nodeId", null,
                new PageParameter(1, 10), userDetail, result, dag);

            Assertions.assertEquals(null, actualResult);
        }

        @Test
        void testGetNodeInfoByMigrate_WithTableRename() {
            List<String> tableNames = Arrays.asList("table1", "table2");
            when(taskNodeService.getMigrateTableNames(sourceNode, userDetail)).thenReturn(tableNames);

            Map<String, String> convertMap = new HashMap<>();
            convertMap.put("table1", "renamed_table1");

            Page<MetadataTransformerItemDto> result = new Page<>();
            when(taskNodeService.getMetadataTransformerItemDtoPage(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(result);

            Page<MetadataTransformerItemDto> actualResult = taskNodeService.getNodeInfoByMigrate(
                "taskId", "nodeId", "",
                new PageParameter(1, 10), userDetail, result, dag);

            verify(dag, times(2)).getPreNodes(any());
        }

        @Test
        void testGetNodeInfoByMigrate_WithSearchTableName() {
            List<String> tableNames = Arrays.asList("user_table", "order_table", "product_table");
            when(taskNodeService.getMigrateTableNames(sourceNode, userDetail)).thenReturn(tableNames);
            when(dag.getPreNodes("nodeId")).thenReturn(new LinkedList<>());

            Page<MetadataTransformerItemDto> result = new Page<>();
            when(taskNodeService.getMetadataTransformerItemDtoPage(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(result);

            Page<MetadataTransformerItemDto> actualResult = taskNodeService.getNodeInfoByMigrate(
                "taskId", "nodeId", "user",
                new PageParameter(1, 10), userDetail, result, dag);

            Assertions.assertNotNull(actualResult);
        }

        @Test
        void testGetNodeInfoByMigrate_WithJsNode() {
            List<String> tableNames = Arrays.asList("table1");
            when(taskNodeService.getMigrateTableNames(sourceNode, userDetail)).thenReturn(tableNames);

            MigrateJsProcessorNode jsNode = mock(MigrateJsProcessorNode.class);
            LinkedList<Node<?>> preNodes = new LinkedList<>();
            preNodes.add(jsNode);
            when(dag.getPreNodes("nodeId")).thenReturn(preNodes);

            Page<MetadataTransformerItemDto> result = new Page<>();
            when(taskNodeService.getMetaByJsNode(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(result);

            Page<MetadataTransformerItemDto> actualResult = taskNodeService.getNodeInfoByMigrate(
                "taskId", "nodeId", "",
                new PageParameter(1, 10), userDetail, result, dag);

            verify(taskNodeService, times(1)).getMetaByJsNode(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
        }
    }

    @Nested
    class TableNameReplacementTest {

        @Test
        void testTableNameReplacement_WithReverseMapping() {
            List<String> tableNames = new ArrayList<>(Arrays.asList("table1", "table2", "table3"));
            List<String> currentTableList = Arrays.asList("renamed_table1", "renamed_table2", "table3");
            Map<String, String> reverseConvertTableNameMap = new HashMap<>();
            reverseConvertTableNameMap.put("renamed_table1", "table1");
            reverseConvertTableNameMap.put("renamed_table2", "table2");

            if (!reverseConvertTableNameMap.isEmpty()) {
                List<String> originalTableNames = currentTableList.stream()
                        .map(tableName -> reverseConvertTableNameMap.getOrDefault(tableName, tableName))
                        .collect(Collectors.toList());
                tableNames.clear();
                tableNames.addAll(originalTableNames);
            }

            Assertions.assertEquals(3, tableNames.size());
            Assertions.assertTrue(tableNames.contains("table1"));
            Assertions.assertTrue(tableNames.contains("table2"));
            Assertions.assertTrue(tableNames.contains("table3"));
        }

        @Test
        void testTableNameReplacement_EmptyReverseMapping() {
            List<String> tableNames = new ArrayList<>(Arrays.asList("table1", "table2"));
            List<String> currentTableList = Arrays.asList("table1", "table2");
            Map<String, String> reverseConvertTableNameMap = new HashMap<>();

            if (!reverseConvertTableNameMap.isEmpty()) {
                List<String> originalTableNames = currentTableList.stream()
                        .map(tableName -> reverseConvertTableNameMap.getOrDefault(tableName, tableName))
                        .collect(Collectors.toList());
                tableNames.clear();
                tableNames.addAll(originalTableNames);
            }

            Assertions.assertEquals(2, tableNames.size());
            Assertions.assertTrue(tableNames.contains("table1"));
            Assertions.assertTrue(tableNames.contains("table2"));
        }

        @Test
        void testTableNameReplacement_PartialMapping() {
            List<String> tableNames = new ArrayList<>(Arrays.asList("table1", "table2", "table3"));
            List<String> currentTableList = Arrays.asList("renamed_table1", "table2", "new_table");
            Map<String, String> reverseConvertTableNameMap = new HashMap<>();
            reverseConvertTableNameMap.put("renamed_table1", "table1");

            if (!reverseConvertTableNameMap.isEmpty()) {
                List<String> originalTableNames = currentTableList.stream()
                        .map(tableName -> reverseConvertTableNameMap.getOrDefault(tableName, tableName))
                        .collect(Collectors.toList());
                tableNames.clear();
                tableNames.addAll(originalTableNames);
            }

            Assertions.assertEquals(3, tableNames.size());
            Assertions.assertTrue(tableNames.contains("table1"));
            Assertions.assertTrue(tableNames.contains("table2"));
            Assertions.assertTrue(tableNames.contains("new_table"));
        }
    }
}