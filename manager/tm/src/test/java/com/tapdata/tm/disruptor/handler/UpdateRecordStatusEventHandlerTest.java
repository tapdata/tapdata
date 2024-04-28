package com.tapdata.tm.disruptor.handler;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.service.TaskService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class UpdateRecordStatusEventHandlerTest {
    private UpdateRecordStatusEventHandler handler;
    private TaskService taskService;
    @BeforeEach
    void buildHandler(){
        handler = new UpdateRecordStatusEventHandler();
        taskService = mock(TaskService.class);
        ReflectionTestUtils.setField(handler,"taskService",taskService);
    }
    @Nested
    class TaskAlarmTest{
        @Test
        @DisplayName("test task alarm for task alarm")
        void test1(){
            AlarmService alarmService = mock(AlarmService.class);
            try (MockedStatic<SpringUtil> mb = Mockito
                    .mockStatic(SpringUtil.class)) {
                mb.when(()->SpringUtil.getBean(AlarmService.class)).thenReturn(alarmService);
                SyncTaskStatusDto data = mock(SyncTaskStatusDto.class);
                when(data.getTaskStatus()).thenReturn("stop");
                TaskDto stopTaskDto = mock(TaskDto.class);
                when(taskService.findById(any())).thenReturn(stopTaskDto);
                when(alarmService.checkOpen(stopTaskDto,null, AlarmKeyEnum.TASK_STATUS_STOP, null, data.getUserDetail())).thenReturn(true);
                handler.taskAlarm(data);
                verify(alarmService, new Times(1)).save(any());
            }
        }
        @Test
        @DisplayName("test task alarm for task running")
        void test2(){
            AlarmService alarmService = mock(AlarmService.class);
            try (MockedStatic<SpringUtil> mb = Mockito
                    .mockStatic(SpringUtil.class)) {
                mb.when(()->SpringUtil.getBean(AlarmService.class)).thenReturn(alarmService);
                SyncTaskStatusDto data = mock(SyncTaskStatusDto.class);
                when(data.getTaskStatus()).thenReturn("running");
                handler.taskAlarm(data);
                verify(alarmService, new Times(1)).closeWhenTaskRunning(any());
            }
        }
        @Test
        @DisplayName("test task alarm for task error")
        void test3(){
            AlarmService alarmService = mock(AlarmService.class);
            try (MockedStatic<SpringUtil> mb = Mockito
                    .mockStatic(SpringUtil.class)) {
                mb.when(()->SpringUtil.getBean(AlarmService.class)).thenReturn(alarmService);
                SyncTaskStatusDto data = mock(SyncTaskStatusDto.class);
                when(data.getTaskStatus()).thenReturn("error");
                TaskDto taskDto = mock(TaskDto.class);
                when(taskService.findById(any())).thenReturn(taskDto);
                when(alarmService.checkOpen(taskDto,null, AlarmKeyEnum.TASK_STATUS_ERROR, null, data.getUserDetail())).thenReturn(true);
                handler.taskAlarm(data);
                verify(alarmService, new Times(1)).save(any());
            }
        }
    }

    @Nested
    class logCollectorAlarmTest{
        @Test
        @DisplayName("Shared mining task stopped")
        void test1(){
            SyncTaskStatusDto syncTaskStatusDto = new SyncTaskStatusDto();
            syncTaskStatusDto.setTaskId(new ObjectId().toHexString());
            syncTaskStatusDto.setTaskName("test");
            syncTaskStatusDto.setTaskStatus("stop");
            TaskDto taskDto = new TaskDto();
            List<Node> nodeList = new ArrayList<>();
            LogCollectorNode logCollectorNode = new LogCollectorNode();
            List<String> ids = new ArrayList<>();
            ids.add("test");
            logCollectorNode.setId("source123");
            logCollectorNode.setConnectionIds(ids);
            TableNode tableNode2 = new TableNode();
            tableNode2.setId("target123");
            tableNode2.setDmlPolicy(new DmlPolicy());
            nodeList.add(tableNode2);
            nodeList.add(logCollectorNode);
            Dag dag = new Dag();
            Edge edge=new Edge("source123","target123");
            List<Edge> edges = Arrays.asList(edge);
            dag.setEdges(edges);
            dag.setNodes(nodeList);
            DAG mockDag =  DAG.build(dag);
            taskDto.setDag(mockDag);
            taskDto.setRestartFlag(false);
            when(taskService.findById(any(),any(Field.class))).thenReturn(taskDto);
            when(taskService.updateMany(any(),any())).thenAnswer(invocationOnMock -> {
                Update update = invocationOnMock.getArgument(1);
                Assertions.assertNotNull(update.getUpdateObject().get("$set"));
                return null;
            });
            handler.logCollectorAlarm(syncTaskStatusDto);
        }

        @Test
        @DisplayName("Shared mining merge")
        void test2(){
            SyncTaskStatusDto syncTaskStatusDto = new SyncTaskStatusDto();
            syncTaskStatusDto.setTaskId(new ObjectId().toHexString());
            syncTaskStatusDto.setTaskName("test");
            syncTaskStatusDto.setTaskStatus("stop");
            TaskDto taskDto = new TaskDto();
            List<Node> nodeList = new ArrayList<>();
            LogCollectorNode logCollectorNode = new LogCollectorNode();
            List<String> ids = new ArrayList<>();
            ids.add("test");
            logCollectorNode.setId("source123");
            logCollectorNode.setConnectionIds(ids);
            TableNode tableNode2 = new TableNode();
            tableNode2.setId("target123");
            tableNode2.setDmlPolicy(new DmlPolicy());
            nodeList.add(tableNode2);
            nodeList.add(logCollectorNode);
            Dag dag = new Dag();
            Edge edge=new Edge("source123","target123");
            List<Edge> edges = Arrays.asList(edge);
            dag.setEdges(edges);
            dag.setNodes(nodeList);
            DAG mockDag =  DAG.build(dag);
            taskDto.setDag(mockDag);
            taskDto.setRestartFlag(true);
            when(taskService.findById(any(),any(Field.class))).thenReturn(taskDto);
            when(taskService.updateMany(any(),any())).thenAnswer(invocationOnMock -> {
                Update update = invocationOnMock.getArgument(1);
                Assertions.assertNull(update.getUpdateObject().get("$set"));
                return null;
            });
            handler.logCollectorAlarm(syncTaskStatusDto);
        }
    }


}
