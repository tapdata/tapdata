package com.tapdata.tm.task.service.impl;


import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateDateProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateUnionProcessorNode;
import com.tapdata.tm.commons.dag.process.StandardMigrateJsProcessorNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesServiceImpl;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

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
}