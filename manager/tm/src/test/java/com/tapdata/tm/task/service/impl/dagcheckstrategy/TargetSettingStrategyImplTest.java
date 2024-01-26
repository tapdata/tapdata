package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.pdk.apis.entity.Capability;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TargetSettingStrategyImplTest {
    @Nested
    class CheckSyncIndexAndExistDataModeTest {
        private TargetSettingStrategyImpl targetSettingStrategy;
        private TaskDagCheckLogService taskDagCheckLogService;

        private DataSourceService dataSourceService;
        private String taskId;
        private Locale locale;

        private String userId;
        private String name;
        @BeforeEach
        void beforeEach(){
            targetSettingStrategy=spy(TargetSettingStrategyImpl.class);
            taskDagCheckLogService = Mockito.mock(TaskDagCheckLogService.class);
            dataSourceService = Mockito.mock(DataSourceService.class);
            targetSettingStrategy.setTaskDagCheckLogService(taskDagCheckLogService);
            targetSettingStrategy.setDataSourceService(dataSourceService);
            locale=new Locale("CN");
            taskId="123";
            userId="userId";
            name="123";
        }
        @DisplayName("test DatabaseNode Enable SyncIndex")
        @Test
        void testCheckNodeSyncIndex(){
            List<TaskDagCheckLog> result=new ArrayList<>();
            DatabaseNode databaseNode=new DatabaseNode();
            Map<String,Object> nodeConfig=new HashMap<>();
            databaseNode.setId("databaseNodeId");
            nodeConfig.put("syncIndex",true);
            databaseNode.setNodeConfig(nodeConfig);
            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setLog("sync Index");
            when(taskDagCheckLogService.createLog(eq(taskId),eq("databaseNodeId"),eq(userId), eq(Level.WARN),any(),any(),eq(name))).thenReturn(log);
            targetSettingStrategy.checkNodeSyncIndex(locale,taskId,result,userId,databaseNode,name);
            assertEquals(1,result.size());
            assertEquals("sync Index",result.get(0).getLog());
        }
        @DisplayName("test DatabaseNode Disable SyncIndex")
        @Test
        void testCheckNodeSyncIndex1(){
            List<TaskDagCheckLog> result=new ArrayList<>();
            DatabaseNode databaseNode=new DatabaseNode();
            Map<String,Object> nodeConfig=new HashMap<>();
            databaseNode.setId("databaseNodeId");
            nodeConfig.put("syncIndex",false);
            databaseNode.setNodeConfig(nodeConfig);
            targetSettingStrategy.checkNodeSyncIndex(locale,taskId,result,userId,databaseNode,name);
            assertEquals(0,result.size());
        }
        @DisplayName("test TableNode Enable SyncIndex")
        @Test
        void testCheckNodeSyncIndex2(){
            List<TaskDagCheckLog> result=new ArrayList<>();
            TableNode tableNode=new TableNode();
            Map<String,Object> nodeConfig=new HashMap<>();
            tableNode.setId("databaseNodeId");
            nodeConfig.put("syncIndex",true);
            tableNode.setNodeConfig(nodeConfig);
            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setLog("sync Index");
            when(taskDagCheckLogService.createLog(eq(taskId),eq("databaseNodeId"),eq(userId), eq(Level.WARN),any(),any(),eq(name))).thenReturn(log);
            targetSettingStrategy.checkNodeSyncIndex(locale,taskId,result,userId,tableNode,name);
            assertEquals(1,result.size());
            assertEquals("sync Index",result.get(0).getLog());
        }
        @DisplayName("test TableNode Disable SyncIndex")
        @Test
        void testCheckNodeSyncIndex3(){
            List<TaskDagCheckLog> result=new ArrayList<>();
            TableNode tableNode=new TableNode();
            Map<String,Object> nodeConfig=new HashMap<>();
            tableNode.setId("databaseNodeId");
            nodeConfig.put("syncIndex",false);
            tableNode.setNodeConfig(nodeConfig);
            targetSettingStrategy.checkNodeSyncIndex(locale,taskId,result,userId,tableNode,name);
            assertEquals(0,result.size());
        }

        @DisplayName("TableNode drop table mode")
        @Test
        void testDataMode1(){
            List<TaskDagCheckLog> result=new ArrayList<>();
            TableNode tableNode=new TableNode();
            tableNode.setId("tableNodeId");
            tableNode.setExistDataProcessMode("dropTable");
            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setLog("dropTable");
            when(taskDagCheckLogService.createLog(eq(taskId),eq("tableNodeId"),eq(userId), eq(Level.WARN),any(),any(),eq(name))).thenReturn(log);
            targetSettingStrategy.checkNodeExistDataMode(locale,taskId,result,userId,tableNode,name);
            assertEquals(1,result.size());
            assertEquals("dropTable",result.get(0).getLog());
        }
        @DisplayName("TableNode keepdata mode")
        @Test
        void testDataMode2(){
            List<TaskDagCheckLog> result=new ArrayList<>();
            TableNode tableNode=new TableNode();
            tableNode.setId("tableNodeId");
            tableNode.setExistDataProcessMode("keepData");
            targetSettingStrategy.checkNodeExistDataMode(locale,taskId,result,userId,tableNode,name);
            assertEquals(0,result.size());
        }
        @DisplayName("DatabaseNode drop table mode")
        @Test
        void testDataMode3(){
            List<TaskDagCheckLog> result=new ArrayList<>();
            DatabaseNode databaseNode=new DatabaseNode();
            databaseNode.setId("databaseNodeId");
            databaseNode.setExistDataProcessMode("dropTable");
            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setLog("dropTable");
            when(taskDagCheckLogService.createLog(eq(taskId),eq("databaseNodeId"),eq(userId), eq(Level.WARN),any(),any(),eq(name))).thenReturn(log);
            targetSettingStrategy.checkNodeExistDataMode(locale,taskId,result,userId,databaseNode,name);
            assertEquals(1,result.size());
            assertEquals("dropTable",result.get(0).getLog());
        }
        @DisplayName("DatabaseNode drop table mode")
        @Test
        void testDataMode4(){
            List<TaskDagCheckLog> result=new ArrayList<>();
            TableNode tableNode=new TableNode();
            tableNode.setId("databaseNodeId");
            tableNode.setExistDataProcessMode("keepdata");
            targetSettingStrategy.checkNodeExistDataMode(locale,taskId,result,userId,tableNode,name);
            assertEquals(0,result.size());
        }
        @DisplayName("test target Update Field can create index")
        @Test
        void test1(){
            DataParentNode dataParentNode=new DatabaseNode();
            dataParentNode.setId("dataNodeId");
            List<TaskDagCheckLog> result=new ArrayList<>();
            Capability capability=new Capability();
            capability.id("create_index_function");
            DataSourceConnectionDto dto=new DataSourceConnectionDto();
            dto.setCapabilities(Lists.of(capability));
            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setLog("update field can create index");
            when(dataSourceService.findByIdByCheck(MongoUtils.toObjectId("connectionId"))).thenReturn(dto);
            when(taskDagCheckLogService.createLog(eq(taskId),eq("dataNodeId"),eq(userId), eq(Level.WARN),any(),any(),eq(name))).thenReturn(log);
            targetSettingStrategy.checkTargetUpdateField(locale,taskId,result,userId,dataParentNode,name,"connectionId");
            assertEquals(1,result.size());
            assertEquals("update field can create index",result.get(0).getLog());
        }
        @DisplayName("test target Update Field,but target dont't have create_index Capability")
        @Test
        void test2(){
            DataParentNode dataParentNode=new DatabaseNode();
            dataParentNode.setId("dataNodeId");
            List<TaskDagCheckLog> result=new ArrayList<>();
            Capability capability=new Capability();
            capability.id("create_index_function");
            DataSourceConnectionDto dto=new DataSourceConnectionDto();
            dto.setCapabilities(Lists.of(capability));
            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setLog("update field can create index");
            when(dataSourceService.findByIdByCheck(MongoUtils.toObjectId("connectionId"))).thenReturn(dto);
            when(taskDagCheckLogService.createLog(eq(taskId),eq("dataNodeId"),eq(userId), eq(Level.WARN),any(),any(),eq(name))).thenReturn(log);
            targetSettingStrategy.checkTargetUpdateField(locale,taskId,result,userId,dataParentNode,name,"connectionId");
            assertEquals(1,result.size());
            assertEquals("update field can create index",result.get(0).getLog());
        }
        @DisplayName("test target Update Field,but target dont't have create_index Capability")
        @Test
        void test3(){
            DataParentNode dataParentNode=new DatabaseNode();
            dataParentNode.setId("dataNodeId");
            List<TaskDagCheckLog> result=new ArrayList<>();
            Capability capability=new Capability();
            capability.id("batch_read_function");
            DataSourceConnectionDto dto=new DataSourceConnectionDto();
            dto.setCapabilities(Lists.of(capability));
            when(dataSourceService.findByIdByCheck(MongoUtils.toObjectId("connectionId"))).thenReturn(dto);
            targetSettingStrategy.checkTargetUpdateField(locale,taskId,result,userId,dataParentNode,name,"connectionId");
            assertEquals(0,result.size());
        }
    }
}
