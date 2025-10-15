package com.tapdata.tm.base.aop;

import com.google.common.collect.Maps;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.commons.alarm.AlarmComponentEnum;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import org.aspectj.lang.JoinPoint;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class MeasureAOPTest {
    @Nested
    class TaskIncrementDelayAlarmTest{
        private TaskService taskService;
        private AlarmService alarmService;
        private UserService userService;
        private AlarmSettingService alarmSettingService;
        Map<String, Map<String, AtomicInteger>> obsMap = Maps.newConcurrentMap();
        String taskId = "66837fb973828322f83f7074";
        List<AlarmInfo> alarmInfos=new ArrayList<>();

        @BeforeEach
        void setUp() {
            taskService = mock(TaskService.class);
            alarmService = mock(AlarmService.class);
            userService = mock(UserService.class);
            alarmSettingService = mock(AlarmSettingService.class);
            Map<String,AtomicInteger> infoMap=new HashMap<>();
            infoMap.put(taskId+"-replicateLag",new AtomicInteger(50000));
            obsMap.put(taskId,infoMap);
            AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE).status(AlarmStatusEnum.ING).type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).metric(AlarmKeyEnum.TASK_INCREMENT_DELAY).firstOccurrenceTime(new Date(System.currentTimeMillis()-50000)).build();
            alarmInfos.add(alarmInfo);
            when(alarmService.find(anyString(), any(), eq(AlarmKeyEnum.TASK_INCREMENT_DELAY))).thenReturn(alarmInfos);
        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is greater and replicateLag not greater alarmRule ms then taskReplicateLagCount is less than alarmRule point")
        @Test
        void test1(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService,alarmSettingService);
            ReflectionTestUtils.setField(measureAOP, "obsMap", obsMap);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.RECOVERY,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.RECOVER,alarmInfo.getStatus());
                assertEquals("TASK_INCREMENT_DELAY_RECOVER",alarmInfo.getSummary());
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto,taskId,10,alarmRuleDto);

        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is greater and replicateLag greater alarmRule ms and taskReplicateLagCount is greater than alarmRule point")
        @Test
        void test2(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService,alarmSettingService);
            ReflectionTestUtils.setField(measureAOP, "obsMap", obsMap);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.WARNING,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.ING,alarmInfo.getStatus());
                Map<String, Object> param = alarmInfo.getParam();
                String flag = param.get("flag").toString();
                assertEquals(MeasureAOP.GREATER,flag);
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto, taskId, 500000, alarmRuleDto);
        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is greater and replicateLag greater alarmRule ms and and info map is null ,so taskReplicateLagCount is less than alarmRule point")
        @Test
        void test3(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService,alarmSettingService);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.RECOVERY,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.RECOVER,alarmInfo.getStatus());
                assertEquals("TASK_INCREMENT_DELAY_RECOVER",alarmInfo.getSummary());
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto, taskId, 500000, alarmRuleDto);
        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is less and replicateLag less alarmRule ms and and info map is not null ,so taskReplicateLagCount is greater than alarmRule point")
        @Test
        void test4(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            alarmRuleDto.setEqualsFlag(-1);
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService,alarmSettingService);
            ReflectionTestUtils.setField(measureAOP, "obsMap", obsMap);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.WARNING,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.ING,alarmInfo.getStatus());
                Map<String, Object> param = alarmInfo.getParam();
                String flag = param.get("flag").toString();
                assertEquals(MeasureAOP.LESS,flag);
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto, taskId, 3, alarmRuleDto);
        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is less and replicateLag less alarmRule ms and and info map is not null ,so taskReplicateLagCount is greater than alarmRule point and continueTime greater 1 MINUTE")
        @Test
        void test5(){
            AlarmInfo listAlarmInfo = alarmInfos.get(0);
            listAlarmInfo.setFirstOccurrenceTime(new Date(System.currentTimeMillis()-1000000000));
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            alarmRuleDto.setEqualsFlag(-1);
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService,alarmSettingService);
            ReflectionTestUtils.setField(measureAOP, "obsMap", obsMap);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.WARNING,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.ING,alarmInfo.getStatus());
                Map<String, Object> param = alarmInfo.getParam();
                String flag = param.get("flag").toString();
                assertEquals(MeasureAOP.LESS,flag);
                assertEquals("TASK_INCREMENT_DELAY_ALWAYS",alarmInfo.getSummary());
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto, taskId, 3, alarmRuleDto);
        }
    }
    @Nested
    class AddAgentMeasurementTest{
        private TaskService taskService;
        private AlarmService alarmService;
        private UserService userService;
        private AlarmSettingService alarmSettingService;
        private MeasureAOP measureAOP;
        private TaskDto taskDto;
        UserDetail userDetail;

        AlarmRuleDto alarmRuleDto;

        public String taskId = "6682d405494d8d2a5c957f38";
        public String userId = "testUserId";


        @BeforeEach
        void setUp() {
            taskService = mock(TaskService.class);
            alarmService = mock(AlarmService.class);
            userService = mock(UserService.class);
            measureAOP = mock(MeasureAOP.class);
            alarmSettingService = mock(AlarmSettingService.class);
            ReflectionTestUtils.setField(measureAOP, "taskService", taskService);
            ReflectionTestUtils.setField(measureAOP, "alarmService", alarmService);
            ReflectionTestUtils.setField(measureAOP, "userService", userService);
            ReflectionTestUtils.setField(measureAOP, "alarmSettingService", alarmSettingService);

            taskDto = new TaskDto();
            taskDto.setId(MongoUtils.toObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            when(taskService.findByTaskId(any())).thenReturn(taskDto);
            alarmRuleDto = getAlarmRuleDto();
            List<AlarmRuleDto> alarmRuleDtoList=new ArrayList<>();
            alarmRuleDtoList.add(alarmRuleDto);

            Map<String, List<AlarmRuleDto>> ruleMap=new HashMap<>();
            ruleMap.put(taskId,alarmRuleDtoList);
            ruleMap.put("test", alarmRuleDtoList);
            when(alarmService.getAlarmRuleDtos(any())).thenReturn(ruleMap);

            userDetail = mock(UserDetail.class);
            when(userService.loadUserById(any())).thenReturn(userDetail);


        }
        @DisplayName("test addAgentMeasurement when sample type is task and have alarmRule is TASK_INCREMENT_DELAY")
        @Test
        void test1(){
            when(alarmService.checkOpen(any(),any(),any(),any(),anyList())).thenReturn(true);
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status")).thenReturn(taskDto);
            List<SampleRequest> list = new ArrayList<>();
            SampleRequest sampleRequest=new SampleRequest();

            Map<String, String> tags =new HashMap<>();
            tags.put("type","task");
            tags.put("taskId",taskId);
            sampleRequest.setTags(tags);

            Map<String,Number> vs=new HashMap<>();
            vs.put("replicateLag",20000);
            Sample sample=new Sample();
            sample.setVs(vs);
            sampleRequest.setSample(sample);

            list.add(sampleRequest);

            JoinPoint joinPoint = mock(JoinPoint.class);

            Object[] objects = new Object[10];
            objects[0]=list;
            when(joinPoint.getArgs()).thenReturn(objects);
            doCallRealMethod().when(measureAOP).addAgentMeasurement(joinPoint);
            measureAOP.addAgentMeasurement(joinPoint);
            verify(measureAOP,times(1)).taskIncrementDelayAlarm(taskDto,taskId, 20000, alarmRuleDto);
        }

        @DisplayName("test addAgentMeasurement when sample type is node")
        @Test
        void test2(){
            DAG dag = mock(DAG.class);
            List<Node> nodes = new ArrayList<>();
            TableNode node = new TableNode();
            node.setId("test");
            node.setName("table");
            nodes.add(node);
            when(dag.getSources()).thenReturn(nodes);
            when(dag.getNode("test")).thenReturn((Node)node);
            taskDto.setDag(dag);
            when(alarmService.checkOpen(any(),any(),any(),any(),anyList())).thenReturn(true);
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status")).thenReturn(taskDto);
            List<SampleRequest> list = new ArrayList<>();
            SampleRequest sampleRequest=new SampleRequest();

            Map<String, String> tags =new HashMap<>();
            tags.put("type","node");
            tags.put("taskId",taskId);
            tags.put("nodeId","test");
            sampleRequest.setTags(tags);

            Map<String,Number> vs=new HashMap<>();
            vs.put("replicateLag",20000);
            Sample sample=new Sample();
            sample.setVs(vs);
            sampleRequest.setSample(sample);

            list.add(sampleRequest);

            JoinPoint joinPoint = mock(JoinPoint.class);

            Object[] objects = new Object[10];
            objects[0]=list;
            when(joinPoint.getArgs()).thenReturn(objects);
            doCallRealMethod().when(measureAOP).addAgentMeasurement(joinPoint);
            measureAOP.addAgentMeasurement(joinPoint);
            verify(measureAOP,times(1)).supplmentDelayAvg(any(),any(),any(),any(),any(),any(),any());
        }

        @DisplayName("test addAgentMeasurement when sample type is node")
        @Test
        void test3(){
            DAG dag = mock(DAG.class);
            List<Node> nodes = new ArrayList<>();
            TableNode node = new TableNode();
            node.setId("test");
            node.setName("table");
            nodes.add(node);
            when(dag.getTargets()).thenReturn(nodes);
            when(dag.getNode("test")).thenReturn((Node)node);
            taskDto.setDag(dag);
            when(alarmService.checkOpen(any(),any(),any(),any(),anyList())).thenReturn(true);
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status")).thenReturn(taskDto);
            List<SampleRequest> list = new ArrayList<>();
            SampleRequest sampleRequest=new SampleRequest();

            Map<String, String> tags =new HashMap<>();
            tags.put("type","node");
            tags.put("taskId",taskId);
            tags.put("nodeId","test");
            sampleRequest.setTags(tags);

            Map<String,Number> vs=new HashMap<>();
            vs.put("replicateLag",20000);
            Sample sample=new Sample();
            sample.setVs(vs);
            sampleRequest.setSample(sample);

            list.add(sampleRequest);

            JoinPoint joinPoint = mock(JoinPoint.class);

            Object[] objects = new Object[10];
            objects[0]=list;
            when(joinPoint.getArgs()).thenReturn(objects);
            doCallRealMethod().when(measureAOP).addAgentMeasurement(joinPoint);
            measureAOP.addAgentMeasurement(joinPoint);
            verify(measureAOP,times(1)).supplmentDelayAvg(any(),any(),any(),any(),any(),any(),any());
        }

        @DisplayName("test addAgentMeasurement when sample type is node")
        @Test
        void test4(){
            DAG dag = mock(DAG.class);
            TableNode node = new TableNode();
            node.setId("test");
            node.setName("table");
            when(dag.getNode("test")).thenReturn((Node)node);
            taskDto.setDag(dag);
            when(alarmService.checkOpen(any(),any(),any(),any(),anyList())).thenReturn(true);
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status")).thenReturn(taskDto);
            List<SampleRequest> list = new ArrayList<>();
            SampleRequest sampleRequest=new SampleRequest();

            Map<String, String> tags =new HashMap<>();
            tags.put("type","node");
            tags.put("taskId",taskId);
            tags.put("nodeId","test");
            sampleRequest.setTags(tags);

            Map<String,Number> vs=new HashMap<>();
            vs.put("replicateLag",20000);
            Sample sample=new Sample();
            sample.setVs(vs);
            sampleRequest.setSample(sample);

            list.add(sampleRequest);

            JoinPoint joinPoint = mock(JoinPoint.class);

            Object[] objects = new Object[10];
            objects[0]=list;
            when(joinPoint.getArgs()).thenReturn(objects);
            doCallRealMethod().when(measureAOP).addAgentMeasurement(joinPoint);
            measureAOP.addAgentMeasurement(joinPoint);
            verify(measureAOP,times(1)).supplmentDelayAvg(any(),any(),any(),any(),any(),any(),any());
        }
    }

    @NotNull
    private static AlarmRuleDto getAlarmRuleDto() {
        AlarmRuleDto alarmRuleDto = new AlarmRuleDto();
        alarmRuleDto.setKey(AlarmKeyEnum.TASK_INCREMENT_DELAY);
        alarmRuleDto.setPoint(24);
        alarmRuleDto.setEqualsFlag(1);
        alarmRuleDto.setMs(40000);
        return alarmRuleDto;
    }
    @Nested
    class SetTaskSnapshotDateTest{
        AlarmService alarmService;
        MeasureAOP measureAOP;
        TaskService taskService;
        @BeforeEach
        void setUp(){
            alarmService = mock(AlarmService.class);
            taskService = mock(TaskService.class);
            measureAOP = new MeasureAOP(taskService,alarmService,null,null);
        }

        @Test
        void test_main(){
            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotStartAt", 123456789);
            vs.put("snapshotDoneAt", 123456789);
            vs.put("currentEventTimestamp", 123456789);
            when(alarmService.checkOpen(any(TaskDto.class),any(),any(),any(),any(UserDetail.class))).thenReturn(false);
            TaskDto taskDto = new TaskDto();
            taskDto.setSnapshotDoneAt(123456789L);
            taskDto.setCurrentEventTimestamp(123456789L);
            UpdateResult updateResult = mock(UpdateResult.class);
            when(taskService.update(any(),any(Update.class))).thenReturn(updateResult);
            measureAOP.setTaskSnapshotDate(vs,"test",taskDto,mock(UserDetail.class));
            verify(taskService,times(1)).update(any(Query.class),any(Update.class));

        }

    }
}
