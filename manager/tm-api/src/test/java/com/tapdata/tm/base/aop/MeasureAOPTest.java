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
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.TimeUtil;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import org.aspectj.lang.JoinPoint;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.mockito.ArgumentCaptor;
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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class MeasureAOPTest {
    @Nested
    class SourceNoIncrementalEventAlarmTest {
        private TaskService taskService;
        private AlarmService alarmService;
        private DataSourceService dataSourceService;
        private MeasureAOP measureAOP;
        private TaskDto taskDto;
        private final String taskId = "66837fb973828322f83f7074";
        private final String nodeId = "source-node";

        @BeforeEach
        void setUp() {
            taskService = mock(TaskService.class);
            alarmService = mock(AlarmService.class);
            dataSourceService = mock(DataSourceService.class);
            measureAOP = new MeasureAOP(taskService, alarmService, null, null, dataSourceService);
            taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setStatus(TaskDto.STATUS_RUNNING);
            taskDto.setName("task-name");
            taskDto.setAgentId("agent-id");
            taskDto.setUserId("user-id");
            DatabaseNode sourceNode = new DatabaseNode();
            sourceNode.setId(nodeId);
            sourceNode.setConnectionId("66337fb973828322f83f7075");
            sourceNode.setName("source");
            taskDto.setDag(DAG.build(new Dag(Collections.emptyList(), Collections.singletonList(sourceNode))));
            DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();
            connectionDto.setHeartbeatEnable(true);
            when(dataSourceService.findById(new ObjectId("66337fb973828322f83f7075"), "heartbeatEnable")).thenReturn(connectionDto);
            when(alarmService.checkOpen(any(TaskDto.class), anyString(), eq(AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT), isNull(), anyList()))
                    .thenReturn(true);
        }

        @Test
        void testStartAlarmWhenNoIncrementalEventForSixtySeconds() {
            Map<String, Number> vs = new HashMap<>();
            long monitorStartAt = System.currentTimeMillis() - 121000L;
            vs.put(MetricCons.SS.VS.F_SOURCE_INCREMENTAL_MONITOR_START_AT, monitorStartAt);
            when(alarmService.find(taskId, nodeId, AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT)).thenReturn(Collections.emptyList());

            measureAOP.sourceNoIncrementalEventAlarm(taskDto, taskId, nodeId, "source", vs,
                    taskDto.getDag().getNode(nodeId), new HashMap<>(), new ArrayList<>());

            verify(alarmService, times(1)).save(argThat(info ->
                    AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT == info.getMetric()
                            && AlarmStatusEnum.ING == info.getStatus()
                            && "TASK_SOURCE_NO_INCREMENTAL_EVENT_START".equals(info.getSummary())
                            && TimeUtil.hhmmssToSeconds(info.getParam().get("idleDuration").toString()) >= 120L));
        }

        @Test
        void testRecoverAlarmWhenDownstreamPendingIncrementalEventExists() {
            Map<String, Number> vs = new HashMap<>();
            long lastCapturedAt = System.currentTimeMillis() - 61000L;
            vs.put(MetricCons.SS.VS.F_LAST_CAPTURED_INCREMENTAL_EVENT_AT, lastCapturedAt);
            vs.put(MetricCons.SS.VS.F_PENDING_INCREMENTAL_EVENT, 1);
            AlarmInfo alarmInfo = AlarmInfo.builder()
                    .status(AlarmStatusEnum.ING)
                    .metric(AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT)
                    .build();
            alarmInfo.setId(new ObjectId());
            when(alarmService.find(taskId, nodeId, AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT)).thenReturn(Collections.singletonList(alarmInfo));

            measureAOP.sourceNoIncrementalEventAlarm(taskDto, taskId, nodeId, "source", vs,
                    taskDto.getDag().getNode(nodeId), new HashMap<>(), new ArrayList<>());

            verify(alarmService, times(1)).save(argThat(info ->
                    AlarmStatusEnum.RECOVER == info.getStatus()
                            && "TASK_SOURCE_NO_INCREMENTAL_EVENT_RECOVER".equals(info.getSummary())));
        }

        @Test
        void testSkipAlarmWhenHeartbeatDisabled() {
            Map<String, Number> vs = new HashMap<>();
            long monitorStartAt = System.currentTimeMillis() - 61000L;
            vs.put(MetricCons.SS.VS.F_SOURCE_INCREMENTAL_MONITOR_START_AT, monitorStartAt);
            DataSourceConnectionDto connectionDto = new DataSourceConnectionDto();
            connectionDto.setHeartbeatEnable(false);
            when(dataSourceService.findById(new ObjectId("66337fb973828322f83f7075"), "heartbeatEnable")).thenReturn(connectionDto);

            measureAOP.sourceNoIncrementalEventAlarm(taskDto, taskId, nodeId, "source", vs,
                    taskDto.getDag().getNode(nodeId), new HashMap<>(), new ArrayList<>());

            verify(alarmService, never()).save(any());
        }

        @Test
        void testSkipAlarmWhenMonitorStartMissingAndLastCapturedInvalid() {
            Map<String, Number> vs = new HashMap<>();
            vs.put(MetricCons.SS.VS.F_LAST_CAPTURED_INCREMENTAL_EVENT_AT, 0L);

            assertDoesNotThrow(() -> measureAOP.sourceNoIncrementalEventAlarm(taskDto, taskId, nodeId, "source", vs,
                    taskDto.getDag().getNode(nodeId), new HashMap<>(), new ArrayList<>()));

            verify(alarmService, never()).find(anyString(), anyString(), eq(AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT));
            verify(alarmService, never()).save(any());
        }

        @Test
        void testSkipAlarmWhenRestartedWithStaleCapturedTimestamp() {
            Map<String, Number> vs = new HashMap<>();
            long now = System.currentTimeMillis();
            vs.put(MetricCons.SS.VS.F_SOURCE_INCREMENTAL_MONITOR_START_AT, now - 1000L);
            vs.put(MetricCons.SS.VS.F_LAST_CAPTURED_INCREMENTAL_EVENT_AT, now - 61000L);
            when(alarmService.find(taskId, nodeId, AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT)).thenReturn(Collections.emptyList());

            measureAOP.sourceNoIncrementalEventAlarm(taskDto, taskId, nodeId, "source", vs,
                    taskDto.getDag().getNode(nodeId), new HashMap<>(), new ArrayList<>());

            verify(alarmService, never()).save(any());
        }

        @Test
        void testIngAlarmPreferredWhenRecoveredAlarmReturnedFirst() {
            Map<String, Number> vs = new HashMap<>();
            vs.put(MetricCons.SS.VS.F_SOURCE_INCREMENTAL_MONITOR_START_AT, System.currentTimeMillis() - 61000L);
            AlarmInfo recoverAlarm = AlarmInfo.builder()
                    .status(AlarmStatusEnum.RECOVER)
                    .metric(AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT)
                    .build();
            AlarmInfo ingAlarm = AlarmInfo.builder()
                    .status(AlarmStatusEnum.ING)
                    .metric(AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT)
                    .build();
            when(alarmService.find(taskId, nodeId, AlarmKeyEnum.TASK_SOURCE_NO_INCREMENTAL_EVENT)).thenReturn(Arrays.asList(recoverAlarm, ingAlarm));

            measureAOP.sourceNoIncrementalEventAlarm(taskDto, taskId, nodeId, "source", vs,
                    taskDto.getDag().getNode(nodeId), new HashMap<>(), new ArrayList<>());

            verify(alarmService, never()).save(any());
        }
    }

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
            DataSourceService dataSourceService = mock(DataSourceService.class);
            Map<String,AtomicInteger> infoMap=new HashMap<>();
            infoMap.put(taskId+"-replicateLag",new AtomicInteger(50000));
            obsMap.put(taskId,infoMap);
            AlarmInfo alarmInfo = AlarmInfo.builder().component(AlarmComponentEnum.FE).status(AlarmStatusEnum.ING).type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).metric(AlarmKeyEnum.TASK_INCREMENT_DELAY).firstOccurrenceTime(new Date(System.currentTimeMillis()-50000)).build();
            alarmInfos.add(alarmInfo);
            when(alarmService.find(anyString(), any(), eq(AlarmKeyEnum.TASK_INCREMENT_DELAY))).thenReturn(alarmInfos);
            when(dataSourceService.findById(any(ObjectId.class), anyString())).thenReturn(new DataSourceConnectionDto());
        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is greater and replicateLag not greater alarmRule ms then taskReplicateLagCount is less than alarmRule point")
        @Test
        void test1(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService,alarmSettingService, null);
            ReflectionTestUtils.setField(measureAOP, "obsMap", obsMap);
            doAnswer(invocationOnMock -> {
                AlarmInfo alarmInfo = (AlarmInfo)invocationOnMock.getArgument(0);
                assertEquals(Level.RECOVERY,alarmInfo.getLevel());
                assertEquals(AlarmStatusEnum.RECOVER,alarmInfo.getStatus());
                assertEquals("TASK_INCREMENT_DELAY_RECOVER",alarmInfo.getSummary());
                return null;
            }).when(alarmService).save(any());
            measureAOP.taskIncrementDelayAlarm(taskDto,taskId,10,alarmRuleDto);
            verify(taskService).updateTaskIncrementDelayAlarm(MongoUtils.toObjectId(taskId), null, null);

        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is greater and replicateLag greater alarmRule ms and taskReplicateLagCount is greater than alarmRule point")
        @Test
        void test2(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService,alarmSettingService, null);
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
            verify(taskService).updateTaskIncrementDelayAlarm(MongoUtils.toObjectId(taskId), 500000L, 40000L);
        }
        @DisplayName("test taskIncrementDelayAlarm method when equalsFlag is greater and replicateLag greater alarmRule ms and and info map is null ,so taskReplicateLagCount is less than alarmRule point")
        @Test
        void test3(){
            AlarmRuleDto alarmRuleDto = getAlarmRuleDto();
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId(taskId));
            taskDto.setCurrentEventTimestamp(System.currentTimeMillis());
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService,alarmSettingService, null);
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
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService,alarmSettingService, null);
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
            verify(taskService).updateTaskIncrementDelayAlarm(MongoUtils.toObjectId(taskId), 3L, 40000L);
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
            MeasureAOP measureAOP = new MeasureAOP(taskService, alarmService, userService,alarmSettingService, null);
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
            verify(taskService).updateTaskIncrementDelayAlarm(MongoUtils.toObjectId(taskId), 3L, 40000L);
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
        public String userId = "6682d405494d8d2a5c957f39";


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
            taskDto.setUserId(userId);
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
            when(userDetail.getUserId()).thenReturn(userId);


        }
        @DisplayName("test addAgentMeasurement when sample type is task and have alarmRule is TASK_INCREMENT_DELAY")
        @Test
        void test1(){
            when(alarmService.checkOpen(any(),any(),any(),any(),anyList())).thenReturn(true);
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status","desc","nodeCurrentEventTimestamp","taskRecordId")).thenReturn(taskDto);
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
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status","desc","nodeCurrentEventTimestamp","taskRecordId")).thenReturn(taskDto);
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

        @DisplayName("test source no incremental event alarm does not depend on node alarm rules")
        @Test
        void testSourceNoIncrementalEventWithoutNodeRules(){
            DAG dag = mock(DAG.class);
            List<Node> nodes = new ArrayList<>();
            DatabaseNode node = new DatabaseNode();
            node.setId("test");
            node.setName("table");
            nodes.add(node);
            when(dag.getSources()).thenReturn(nodes);
            when(dag.getNode("test")).thenReturn((Node)node);
            taskDto.setDag(dag);
            taskDto.setStatus(TaskDto.STATUS_RUNNING);
            when(alarmService.getAlarmRuleDtos(any())).thenReturn(Collections.emptyMap());
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status","desc","nodeCurrentEventTimestamp","taskRecordId")).thenReturn(taskDto);
            List<SampleRequest> list = new ArrayList<>();
            SampleRequest sampleRequest=new SampleRequest();

            Map<String, String> tags =new HashMap<>();
            tags.put("type","node");
            tags.put("taskId",taskId);
            tags.put("nodeId","test");
            sampleRequest.setTags(tags);

            Sample sample=new Sample();
            sample.setVs(new HashMap<>());
            sampleRequest.setSample(sample);

            list.add(sampleRequest);

            JoinPoint joinPoint = mock(JoinPoint.class);

            Object[] objects = new Object[10];
            objects[0]=list;
            when(joinPoint.getArgs()).thenReturn(objects);
            doCallRealMethod().when(measureAOP).addAgentMeasurement(joinPoint);
            measureAOP.addAgentMeasurement(joinPoint);
            verify(measureAOP,times(1)).sourceNoIncrementalEventAlarm(eq(taskDto), eq(taskId), eq("test"), eq("table"), any(), any(), anyMap(), any());
            verify(measureAOP,never()).supplmentDelayAvg(any(),any(),any(),any(),any(),any(),any());
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
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status","desc","nodeCurrentEventTimestamp","taskRecordId")).thenReturn(taskDto);
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
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status","desc","nodeCurrentEventTimestamp","taskRecordId")).thenReturn(taskDto);
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

        @DisplayName("test addAgentMeasurement skips sample when user detail is missing")
        @Test
        void testSkipWhenUserDetailMissing(){
            when(userService.loadUserById(any())).thenReturn(null);
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status","desc","nodeCurrentEventTimestamp","taskRecordId")).thenReturn(taskDto);
            List<SampleRequest> list = new ArrayList<>();
            SampleRequest sampleRequest=new SampleRequest();

            Map<String, String> tags =new HashMap<>();
            tags.put("type","task");
            tags.put("taskId",taskId);
            sampleRequest.setTags(tags);

            Sample sample=new Sample();
            sample.setVs(new HashMap<>());
            sampleRequest.setSample(sample);
            list.add(sampleRequest);

            JoinPoint joinPoint = mock(JoinPoint.class);
            Object[] objects = new Object[10];
            objects[0]=list;
            when(joinPoint.getArgs()).thenReturn(objects);
            doCallRealMethod().when(measureAOP).addAgentMeasurement(joinPoint);

            assertDoesNotThrow(() -> measureAOP.addAgentMeasurement(joinPoint));
            verify(measureAOP, never()).setTaskSnapshotDate(any(), any(), any(), any());
        }

        @DisplayName("test addAgentMeasurement skips node sample when dag node is missing")
        @Test
        void testSkipWhenDagNodeMissing(){
            DAG dag = mock(DAG.class);
            when(dag.getSources()).thenReturn(Collections.emptyList());
            when(dag.getTargets()).thenReturn(Collections.emptyList());
            when(dag.getNode("missing")).thenReturn(null);
            taskDto.setDag(dag);
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId),"_id","dag","user_id","agentId","name","currentEventTimestamp","alarmSettings","alarmRules","snapshotDoneAt","status","desc","nodeCurrentEventTimestamp","taskRecordId")).thenReturn(taskDto);
            List<SampleRequest> list = new ArrayList<>();
            SampleRequest sampleRequest=new SampleRequest();

            Map<String, String> tags =new HashMap<>();
            tags.put("type","node");
            tags.put("taskId",taskId);
            tags.put("nodeId","missing");
            sampleRequest.setTags(tags);

            Sample sample=new Sample();
            sample.setVs(new HashMap<>());
            sampleRequest.setSample(sample);
            list.add(sampleRequest);

            JoinPoint joinPoint = mock(JoinPoint.class);
            Object[] objects = new Object[10];
            objects[0]=list;
            when(joinPoint.getArgs()).thenReturn(objects);
            doCallRealMethod().when(measureAOP).addAgentMeasurement(joinPoint);

            assertDoesNotThrow(() -> measureAOP.addAgentMeasurement(joinPoint));
            verify(measureAOP, never()).sourceNoIncrementalEventAlarm(any(), any(), any(), any(), any(), any(), any(), any());
            verify(measureAOP,never()).supplmentDelayAvg(any(),any(),any(),any(),any(),any(),any());
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
            measureAOP = new MeasureAOP(taskService,alarmService,null,null, null);
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

    @Nested
    class ResetMetricWriteGateTest {
        private TaskService taskService;
        private AlarmService alarmService;
        private UserService userService;
        private AlarmSettingService alarmSettingService;
        private MeasureAOP measureAOP;
        private final String taskId = "6682d405494d8d2a5c957f38";
        private final String userId = "6682d405494d8d2a5c957f39";
        private final String CURRENT_RECORD_ID = "current-run-record";
        private final String PREVIOUS_RECORD_ID = "previous-run-record";
        private final String NODE_ID = "source-node";

        @BeforeEach
        void setUp() {
            taskService = mock(TaskService.class);
            alarmService = mock(AlarmService.class);
            userService = mock(UserService.class);
            alarmSettingService = mock(AlarmSettingService.class);
            measureAOP = new MeasureAOP(taskService, alarmService, userService, alarmSettingService, null);
            UserDetail userDetail = mock(UserDetail.class);
            when(userDetail.getUserId()).thenReturn(userId);
            when(userService.loadUserById(any())).thenReturn(userDetail);
            when(alarmSettingService.findAllAlarmSetting(any())).thenReturn(Collections.emptyList());
            when(alarmService.checkOpen(any(TaskDto.class), any(), any(), any(), any(UserDetail.class))).thenReturn(false);
        }

        private TaskDto task(String status, String taskRecordId) {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(MongoUtils.toObjectId(taskId));
            taskDto.setUserId(userId);
            taskDto.setStatus(status);
            taskDto.setTaskRecordId(taskRecordId);
            return taskDto;
        }

        private SampleRequest sample(String type, String taskRecordId, Map<String, Number> vs) {
            SampleRequest sampleRequest = new SampleRequest();
            Map<String, String> tags = new HashMap<>();
            tags.put("type", type);
            tags.put("taskId", taskId);
            if (taskRecordId != null) {
                tags.put("taskRecordId", taskRecordId);
            }
            sampleRequest.setTags(tags);
            Sample sample = new Sample();
            sample.setVs(vs);
            sampleRequest.setSample(sample);
            return sampleRequest;
        }

        private SampleRequest nodeSample(String taskRecordId, long currentEventTimestamp) {
            Map<String, Number> vs = new HashMap<>();
            vs.put("currentEventTimestamp", currentEventTimestamp);
            SampleRequest sampleRequest = sample("node", taskRecordId, vs);
            sampleRequest.getTags().put("nodeId", NODE_ID);
            return sampleRequest;
        }

        private void withSourceNode(TaskDto taskDto) {
            DAG dag = mock(DAG.class);
            TableNode node = new TableNode();
            node.setId(NODE_ID);
            node.setName("source");
            when(dag.getNode(NODE_ID)).thenReturn((Node) node);
            when(dag.getSources()).thenReturn(Collections.singletonList(node));
            taskDto.setDag(dag);
        }

        private void stubFindTask(TaskDto taskDto) {
            when(taskService.findByTaskId(MongoUtils.toObjectId(taskId), "_id", "dag", "user_id", "agentId", "name",
                    "currentEventTimestamp", "alarmSettings", "alarmRules", "snapshotDoneAt", "status", "desc",
                    "nodeCurrentEventTimestamp", "taskRecordId")).thenReturn(taskDto);
        }

        private void stubUpdateMatchedCount(long matchedCount) {
            UpdateResult updateResult = mock(UpdateResult.class);
            when(updateResult.getMatchedCount()).thenReturn(matchedCount);
            when(taskService.update(any(Query.class), any(Update.class))).thenReturn(updateResult);
        }

        private void runBatch(SampleRequest... samples) {
            JoinPoint joinPoint = mock(JoinPoint.class);
            when(joinPoint.getArgs()).thenReturn(new Object[]{new ArrayList<>(Arrays.asList(samples))});
            measureAOP.addAgentMeasurement(joinPoint);
        }

        @Test
        @DisplayName("只有不存在本轮运行的状态才拦截，scheduling / wait_run 期间的本轮样本不能被丢弃")
        void testIsMetricBlockedStatus() {
            assertTrue(measureAOP.isMetricBlockedStatus(task(TaskDto.STATUS_RENEWING, CURRENT_RECORD_ID)));
            assertTrue(measureAOP.isMetricBlockedStatus(task(TaskDto.STATUS_WAIT_START, CURRENT_RECORD_ID)));
            assertTrue(measureAOP.isMetricBlockedStatus(task(TaskDto.STATUS_DELETING, CURRENT_RECORD_ID)));
            assertTrue(measureAOP.isMetricBlockedStatus(task(TaskDto.STATUS_RENEW_FAILED, CURRENT_RECORD_ID)));
            assertTrue(measureAOP.isMetricBlockedStatus(task(TaskDto.STATUS_DELETE_FAILED, CURRENT_RECORD_ID)));
            assertFalse(measureAOP.isMetricBlockedStatus(task(TaskDto.STATUS_SCHEDULING, CURRENT_RECORD_ID)));
            assertFalse(measureAOP.isMetricBlockedStatus(task(TaskDto.STATUS_WAIT_RUN, CURRENT_RECORD_ID)));
            assertFalse(measureAOP.isMetricBlockedStatus(task(TaskDto.STATUS_RUNNING, CURRENT_RECORD_ID)));
            assertFalse(measureAOP.isMetricBlockedStatus(task(TaskDto.STATUS_STOP, CURRENT_RECORD_ID)));
            assertFalse(measureAOP.isMetricBlockedStatus(task(TaskDto.STATUS_STOPPING, CURRENT_RECORD_ID)));
        }

        @Test
        @DisplayName("taskRecordId 不一致才判定为上一轮样本，缺失时不做判定")
        void testIsStaleRunSample() {
            TaskDto taskDto = task(TaskDto.STATUS_RUNNING, CURRENT_RECORD_ID);
            assertTrue(measureAOP.isStaleRunSample(taskDto, sample("task", PREVIOUS_RECORD_ID, new HashMap<>())));
            assertFalse(measureAOP.isStaleRunSample(taskDto, sample("task", CURRENT_RECORD_ID, new HashMap<>())));
            assertFalse(measureAOP.isStaleRunSample(taskDto, sample("task", null, new HashMap<>())));
            assertFalse(measureAOP.isStaleRunSample(task(TaskDto.STATUS_RUNNING, null),
                    sample("task", PREVIOUS_RECORD_ID, new HashMap<>())));
        }

        @Test
        @DisplayName("回写条件同时带上 taskRecordId 栅栏和状态条件，由 MongoDB 原子判定")
        void testWriteSnapshotDoneAtWithAtomicFence() {
            TaskDto taskDto = task(TaskDto.STATUS_STOP, CURRENT_RECORD_ID);
            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotDoneAt", 2000L);
            stubUpdateMatchedCount(1L);

            measureAOP.setTaskSnapshotDate(vs, taskId, taskDto, mock(UserDetail.class));

            ArgumentCaptor<Query> queryCaptor = ArgumentCaptor.forClass(Query.class);
            ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
            verify(taskService, times(1)).update(queryCaptor.capture(), updateCaptor.capture());

            assertEquals(2000L,
                    updateCaptor.getValue().getUpdateObject().get("$set", Document.class).get("snapshotDoneAt"));
            Document queryObject = queryCaptor.getValue().getQueryObject();
            assertEquals(CURRENT_RECORD_ID, queryObject.get("taskRecordId"));
            Collection<?> blockedStatus = (Collection<?>) queryObject.get("status", Document.class).get("$nin");
            assertTrue(blockedStatus.containsAll(Arrays.asList(TaskDto.STATUS_RENEWING, TaskDto.STATUS_WAIT_START,
                    TaskDto.STATUS_DELETING, TaskDto.STATUS_RENEW_FAILED, TaskDto.STATUS_DELETE_FAILED)));
            assertFalse(blockedStatus.contains(TaskDto.STATUS_SCHEDULING));
            assertFalse(blockedStatus.contains(TaskDto.STATUS_WAIT_RUN));
        }

        @Test
        @DisplayName("任务已在 running 且文档已切到新 taskRecordId 时，上一轮样本既不回写也不触发告警")
        void testSkipStaleSampleWhenTaskRunning() {
            TaskDto taskDto = task(TaskDto.STATUS_RUNNING, CURRENT_RECORD_ID);
            withSourceNode(taskDto);
            stubFindTask(taskDto);

            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotDoneAt", 2000L);
            vs.put("replicateLag", 50000L);
            runBatch(sample("task", PREVIOUS_RECORD_ID, vs), nodeSample(PREVIOUS_RECORD_ID, 2000L));

            verify(alarmService, never()).getAlarmRuleDtos(any());
            verify(alarmService, never()).save(any());
            verify(taskService, never()).update(any(Query.class), any(Update.class));
            verify(taskService, never()).updateTaskIncrementDelayAlarm(any(), any(), any());
        }

        @Test
        @DisplayName("renew_failed 重试重置时 taskRecordId 不变，本 id 的样本也不能参与告警判定与回写")
        void testSkipSampleWhenRenewFailedWithSameRecordId() {
            TaskDto taskDto = task(TaskDto.STATUS_RENEW_FAILED, CURRENT_RECORD_ID);
            taskDto.setCurrentEventTimestamp(1000L);
            stubFindTask(taskDto);
            Map<String, List<AlarmRuleDto>> ruleMap = new HashMap<>();
            ruleMap.put(taskId, Collections.singletonList(getAlarmRuleDto()));
            when(alarmService.getAlarmRuleDtos(any())).thenReturn(ruleMap);
            when(alarmService.checkOpen(any(TaskDto.class), any(), any(), any(), anyList())).thenReturn(true);

            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotDoneAt", 2000L);
            vs.put("replicateLag", 50000L);
            runBatch(sample("task", CURRENT_RECORD_ID, vs));

            verify(alarmService, never()).getAlarmRuleDtos(any());
            verify(alarmService, never()).save(any());
            verify(taskService, never()).update(any(Query.class), any(Update.class));
            verify(taskService, never()).updateTaskIncrementDelayAlarm(any(), any(), any());
        }

        @Test
        @DisplayName("禁止状态下即使 taskRecordId 一致，节点样本也不得回写 nodeCurrentEventTimestamp")
        void testSkipNodeSampleWhenBlockedWithSameRecordId() {
            TaskDto taskDto = task(TaskDto.STATUS_WAIT_START, CURRENT_RECORD_ID);
            withSourceNode(taskDto);
            stubFindTask(taskDto);
            stubUpdateMatchedCount(1L);

            runBatch(nodeSample(CURRENT_RECORD_ID, 2000L));

            verify(taskService, never()).update(any(Query.class), any(Update.class));
            assertNull(taskDto.getNodeCurrentEventTimestamp());
        }

        @Test
        @DisplayName("scheduling / wait_run 期间到达的本轮样本仍要回写，避免短任务永久丢失 snapshotDoneAt")
        void testWriteCurrentRunSampleBeforeRunningReported() {
            for (String status : Arrays.asList(TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN)) {
                reset(taskService);
                TaskDto taskDto = task(status, CURRENT_RECORD_ID);
                stubFindTask(taskDto);
                stubUpdateMatchedCount(1L);

                Map<String, Number> vs = new HashMap<>();
                vs.put("snapshotDoneAt", 2000L);
                runBatch(sample("task", CURRENT_RECORD_ID, vs));

                ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
                verify(taskService, times(1)).update(any(Query.class), updateCaptor.capture());
                assertEquals(2000L,
                        updateCaptor.getValue().getUpdateObject().get("$set", Document.class).get("snapshotDoneAt"), status);
            }
        }

        @Test
        @DisplayName("节点回写命中后记入内存，同批相同时间戳的样本不再重复写")
        void testNodeWriteRecordedInMemoryWhenMatched() {
            TaskDto taskDto = task(TaskDto.STATUS_RUNNING, CURRENT_RECORD_ID);
            withSourceNode(taskDto);
            stubFindTask(taskDto);
            stubUpdateMatchedCount(1L);

            runBatch(nodeSample(CURRENT_RECORD_ID, 2000L), nodeSample(CURRENT_RECORD_ID, 2000L));

            verify(taskService, times(1)).update(any(Query.class), any(Update.class));
            assertEquals(2000L, taskDto.getNodeCurrentEventTimestamp().get(NODE_ID));
        }

        @Test
        @DisplayName("节点回写被栅栏拒绝（命中 0 行）时不更新内存，同批后续样本仍会尝试写入")
        void testNodeWriteNotRecordedInMemoryWhenFenceRejected() {
            TaskDto taskDto = task(TaskDto.STATUS_RUNNING, CURRENT_RECORD_ID);
            withSourceNode(taskDto);
            stubFindTask(taskDto);
            stubUpdateMatchedCount(0L);

            runBatch(nodeSample(CURRENT_RECORD_ID, 2000L), nodeSample(CURRENT_RECORD_ID, 2000L));

            verify(taskService, times(2)).update(any(Query.class), any(Update.class));
            assertNull(taskDto.getNodeCurrentEventTimestamp());
        }

        @Test
        @DisplayName("批次中存在没有 tags 的样本时不抛异常，其余样本照常处理")
        void testSkipSampleWithoutTags() {
            TaskDto taskDto = task(TaskDto.STATUS_RUNNING, CURRENT_RECORD_ID);
            stubFindTask(taskDto);
            when(alarmService.getAlarmRuleDtos(any())).thenReturn(Collections.emptyMap());
            stubUpdateMatchedCount(1L);
            SampleRequest tagless = new SampleRequest();
            tagless.setSample(new Sample());

            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotDoneAt", 2000L);
            assertDoesNotThrow(() -> runBatch(tagless, sample("task", CURRENT_RECORD_ID, vs)));

            verify(alarmService, times(1)).getAlarmRuleDtos(any());
            verify(taskService, times(1)).update(any(Query.class), any(Update.class));
        }

        @Test
        @DisplayName("本轮样本在 addAgentMeasurement 中正常参与处理")
        void testProcessCurrentRunSample() {
            TaskDto taskDto = task(TaskDto.STATUS_RUNNING, CURRENT_RECORD_ID);
            stubFindTask(taskDto);
            when(alarmService.getAlarmRuleDtos(any())).thenReturn(Collections.emptyMap());
            stubUpdateMatchedCount(1L);

            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotDoneAt", 2000L);
            runBatch(sample("task", CURRENT_RECORD_ID, vs));

            verify(alarmService, times(1)).getAlarmRuleDtos(any());
            verify(taskService, times(1)).update(any(Query.class), any(Update.class));
        }
    }
}
