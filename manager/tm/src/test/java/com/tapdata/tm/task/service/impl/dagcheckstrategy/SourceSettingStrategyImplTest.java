package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.Dag;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SourceSettingStrategyImplTest {
    @Nested
    class TestCheckDDL{
        private SourceSettingStrategyImpl sourceSettingStrategy;
        private DataSourceService dataSourceService;

        private TaskDagCheckLogService taskDagCheckLogService;
        private String taskId;
        private Locale locale;

        private String userId;
        private String name;

        @BeforeEach
        void beforeEach(){
            sourceSettingStrategy=spy(SourceSettingStrategyImpl.class);
            dataSourceService= Mockito.mock(DataSourceService.class);
            taskDagCheckLogService = Mockito.mock(TaskDagCheckLogService.class);
            sourceSettingStrategy.setDataSourceService(dataSourceService);
            sourceSettingStrategy.setTaskDagCheckLogService(taskDagCheckLogService);
            locale = Locale.CHINA;
            taskId="123";
            userId="userId";
            name="123";
        }
        @DisplayName("test checkDDL normal")
        @Test
        void testCheckDDLNormal(){
            List<TaskDagCheckLog> result =new ArrayList<>();
            TableNode sourceTableNode=new TableNode();
            sourceTableNode.setEnableDDL(true);
            sourceTableNode.setId("source123");
            TableNode targetTableNode=new TableNode();
            targetTableNode.setId("target123");
            targetTableNode.setConnectionId("conId123");
            Edge edge=new Edge("source123","target123");
            Dag dag=new Dag();
            dag.setNodes(Lists.of(sourceTableNode,targetTableNode));
            dag.setEdges(Lists.of(edge));
            DAG.build(dag);
            Capability capability=new Capability();
            capability.id("alter_field_name_function");
            DataSourceConnectionDto dto=new DataSourceConnectionDto();
            dto.setCapabilities(Lists.of(capability));
            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setLog("DDL");
            when(dataSourceService.findByIdByCheck(MongoUtils.toObjectId("conId123"))).thenReturn(dto);
            when(taskDagCheckLogService.createLog(eq(taskId),eq("source123"),eq(userId), eq(Level.WARN),any(),any(),eq(name))).thenReturn(log);
            sourceSettingStrategy.checkDDL(locale,taskId,result,userId,name,sourceTableNode);
            assertTrue(result.size()>0);
            assertEquals("DDL",result.get(0).getLog());
        }
        @DisplayName("test checkDDL target has no DDL capability ")
        @Test
        void testCheckDDL1(){
            List<TaskDagCheckLog> result =new ArrayList<>();
            TableNode sourceTableNode=new TableNode();
            sourceTableNode.setEnableDDL(true);
            sourceTableNode.setId("source123");
            TableNode targetTableNode=new TableNode();
            targetTableNode.setId("target123");
            targetTableNode.setConnectionId("conId123");
            Edge edge=new Edge("source123","target123");
            Dag dag=new Dag();
            dag.setNodes(Lists.of(sourceTableNode,targetTableNode));
            dag.setEdges(Lists.of(edge));
            DAG.build(dag);
            Capability capability=new Capability();
            DataSourceConnectionDto dto=new DataSourceConnectionDto();
            dto.setCapabilities(Lists.of(capability));
            when(dataSourceService.findByIdByCheck(MongoUtils.toObjectId("conId123"))).thenReturn(dto);
            sourceSettingStrategy.checkDDL(locale,taskId,result,userId,name,sourceTableNode);
            assertEquals(0,result.size());
        }
        @DisplayName("test checkDDL ddl is not enabled on the source ")
        @Test
        void testCheckDDL2(){
            List<TaskDagCheckLog> result =new ArrayList<>();
            TableNode sourceTableNode=new TableNode();
            sourceTableNode.setEnableDDL(true);
            sourceTableNode.setId("source123");
            TableNode targetTableNode=new TableNode();
            targetTableNode.setId("target123");
            targetTableNode.setConnectionId("conId123");
            Edge edge=new Edge("source123","target123");
            Dag dag=new Dag();
            dag.setNodes(Lists.of(sourceTableNode,targetTableNode));
            dag.setEdges(Lists.of(edge));
            DAG.build(dag);
            sourceSettingStrategy.checkDDL(locale,taskId,result,userId,name,sourceTableNode);
            assertEquals(0,result.size());
        }

        @DisplayName("test CheckIsFilterOrCustomCommand method is Filter")
        @Test
        void testCheckIsFilterOrCustomCommand(){
            List<TaskDagCheckLog> result =new ArrayList<>();
            TableNode tableNode=new TableNode();
            tableNode.setId("isFileterSouce");
            tableNode.setIsFilter(true);
            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setLog("isFilter");
            when(taskDagCheckLogService.createLog(eq(taskId),eq("isFileterSouce"),eq(userId), eq(Level.WARN),any(),any(),eq(name))).thenReturn(log);
            sourceSettingStrategy.checkIsFilterOrCustomCommand(locale,taskId,result,userId,name,tableNode);
            assertEquals(1,result.size());
            assertEquals("isFilter",result.get(0).getLog());
        }
        @DisplayName("test CheckIsFilterOrCustomCommand method is Command")
        @Test
        void testCheckIsFilterOrCustomCommand1(){
            List<TaskDagCheckLog> result =new ArrayList<>();
            TableNode tableNode=new TableNode();
            tableNode.setId("isCommandSouce");
            tableNode.setEnableCustomCommand(true);
            TaskDagCheckLog log = new TaskDagCheckLog();
            log.setLog("CommandSouce");
            when(taskDagCheckLogService.createLog(eq(taskId),eq("isCommandSouce"),eq(userId), eq(Level.WARN),any(),any(),eq(name))).thenReturn(log);
            sourceSettingStrategy.checkIsFilterOrCustomCommand(locale,taskId,result,userId,name,tableNode);
            assertEquals(1,result.size());
            assertEquals("CommandSouce",result.get(0).getLog());
        }
        @DisplayName("test CheckIsFilterOrCustomCommand method not enable Filter and command")
        @Test
        void testCheckIsFilterOrCustomCommand2(){
            List<TaskDagCheckLog> result =new ArrayList<>();
            TableNode tableNode=new TableNode();
            tableNode.setId("isCommandSouce");
            sourceSettingStrategy.checkIsFilterOrCustomCommand(locale,taskId,result,userId,name,tableNode);
            assertEquals(0,result.size());
        }
    }

}
