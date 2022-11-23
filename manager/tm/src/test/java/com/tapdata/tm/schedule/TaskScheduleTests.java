package com.tapdata.tm.schedule;


import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.TMApplication;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;

import java.lang.reflect.Field;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

@ActiveProfiles("dev")
@SpringBootTest(classes = {TMApplication.class})
public class TaskScheduleTests {

    @Autowired
    protected TaskRepository taskRepository;


    @Autowired
    private TaskScheduleService taskScheduleService;

    @Autowired
    private WorkerService workerService;

    @Autowired
    private TaskService taskService;

    @Autowired
    private UserService userService;

    protected UserDetail user;

    protected ObjectId taskId;



    @MockBean
    private MessageQueueService messageQueueService;


    private String initJson = "{\"id\":\"6375e6e589668816de276a5d\",\"lastUpdBy\":\"62bc5008d4958d013d97c7a6\",\"createUser\":\"admin@admin.com\",\"attrs\":{},\"deduplicWriteMode\":\"intelligent\",\"desc\":\"\",\"increHysteresis\":false,\"increOperationMode\":false,\"increSyncConcurrency\":true,\"processorThreadNum\":1,\"increaseReadSize\":1,\"readBatchSize\":500,\"isAutoCreateIndex\":true,\"isFilter\":false,\"isOpenAutoDDL\":false,\"isSchedule\":false,\"isStopOnError\":true,\"name\":\"新任务@15:46:45\",\"shareCdcEnable\":false,\"statuses\":[],\"status\":\"wait_start\",\"type\":\"initial_sync+cdc\",\"writeThreadSize\":8,\"editVersion\":\"1668681194984\",\"syncPoints\":[{}],\"syncType\":\"sync\",\"transformProcess\":0.0,\"planStartDateFlag\":false,\"accessNodeType\":\"AUTOMATIC_PLATFORM_ALLOCATION\",\"accessNodeProcessIdList\":[],\"accessNodeProcessId\":\"\",\"isEdit\":true,\"scheduledTime\":\"2022-11-17T07:51:40.683+00:00\",\"stoppingTime\":\"2022-11-17T10:22:43.286+00:00\",\"runningTime\":\"2022-11-17T07:51:41.470+00:00\",\"pingTime\":1668680571596,\"restartFlag\":false,\"transformUuid\":\"1668681194924\",\"transformed\":true,\"transformDagHash\":0,\"canForceStopping\":true,\"dag\":{\"edges\":[{\"source\":\"8326bc9a-ae30-4c75-9c17-c27461152977\",\"target\":\"861f38dc-cede-487b-a40a-74f21afe6438\"}],\"nodes\":[{\"tableName\":\"xcxcxczv\",\"isFilter\":true,\"increaseReadSize\":500,\"readBatchSize\":500,\"maxTransactionDuration\":12.0,\"existDataProcessMode\":\"keepData\",\"writeStrategy\":\"updateOrInsert\",\"updateConditionFields\":[\"COLUMN1\"],\"xmlIncludeFile\":false,\"esFragmentNum\":3,\"nodeConfig\":{},\"connectionId\":\"62d7e68b5fcbb347ae36d0b7\",\"databaseType\":\"MongoDB\",\"initialConcurrent\":true,\"initialConcurrentWriteNum\":8,\"type\":\"table\",\"catalog\":\"data\",\"isTransformed\":false,\"alarmSettings\":[{\"type\":\"DATANODE\",\"open\":true,\"key\":\"DATANODE_CANNOT_CONNECT\",\"sort\":1,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"DATANODE\",\"open\":true,\"key\":\"DATANODE_AVERAGE_HANDLE_CONSUME\",\"sort\":4,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"}],\"alarmRules\":[{\"key\":\"DATANODE_AVERAGE_HANDLE_CONSUME\",\"point\":3,\"equalsFlag\":1,\"ms\":500}],\"id\":\"861f38dc-cede-487b-a40a-74f21afe6438\",\"name\":\"xcxcxczv\",\"elementType\":\"Node\",\"attrs\":{\"position\":[60,385],\"connectionName\":\"zed_mongo\",\"connectionType\":\"source_and_target\",\"accessNodeProcessId\":\"\",\"pdkType\":\"pdk\",\"pdkHash\":\"4335aaa005ec1a74a4e2166bded2962e939ad50239f48b023b884f35b54129a5\",\"capabilities\":[{\"type\":11,\"id\":\"batch_read_function\"},{\"type\":11,\"id\":\"stream_read_function\"},{\"type\":11,\"id\":\"batch_count_function\"},{\"type\":11,\"id\":\"timestamp_to_stream_offset_function\"},{\"type\":11,\"id\":\"write_record_function\"},{\"type\":11,\"id\":\"query_by_advance_filter_function\"},{\"type\":11,\"id\":\"drop_table_function\"},{\"type\":11,\"id\":\"create_index_function\"},{\"type\":11,\"id\":\"get_table_names_function\"},{\"type\":11,\"id\":\"error_handle_function\"},{\"type\":20,\"id\":\"master_slave_merge\"},{\"type\":20,\"id\":\"dml_insert_policy\",\"alternatives\":[\"update_on_exists\",\"ignore_on_exists\"]},{\"type\":20,\"id\":\"dml_update_policy\",\"alternatives\":[\"ignore_on_nonexists\",\"insert_on_nonexists\"]},{\"type\":20,\"id\":\"api_server_supported\"}]}},{\"tableName\":\"04120001\",\"isFilter\":true,\"increaseReadSize\":500,\"readBatchSize\":500,\"maxTransactionDuration\":12.0,\"existDataProcessMode\":\"keepData\",\"writeStrategy\":\"updateOrInsert\",\"xmlIncludeFile\":false,\"esFragmentNum\":3,\"connectionId\":\"629053b06436fc5fa5412547\",\"databaseType\":\"Mysql\",\"type\":\"table\",\"catalog\":\"data\",\"isTransformed\":false,\"alarmSettings\":[{\"type\":\"DATANODE\",\"open\":true,\"key\":\"DATANODE_CANNOT_CONNECT\",\"sort\":1,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"DATANODE\",\"open\":true,\"key\":\"DATANODE_AVERAGE_HANDLE_CONSUME\",\"sort\":4,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"}],\"alarmRules\":[{\"key\":\"DATANODE_AVERAGE_HANDLE_CONSUME\",\"point\":3,\"equalsFlag\":1,\"ms\":500}],\"id\":\"8326bc9a-ae30-4c75-9c17-c27461152977\",\"name\":\"04120001\",\"elementType\":\"Node\",\"attrs\":{\"position\":[-338,175],\"connectionName\":\"MySQL-33061\",\"connectionType\":\"source_and_target\",\"accessNodeProcessId\":\"\",\"pdkType\":\"pdk\",\"pdkHash\":\"a5af410b12afca476edf4a650c133ddf135bf76542a67787ed6f7f7d53ba712\",\"capabilities\":[{\"type\":10,\"id\":\"new_field_event\"},{\"type\":10,\"id\":\"alter_field_name_event\"},{\"type\":10,\"id\":\"alter_field_attributes_event\"},{\"type\":10,\"id\":\"drop_field_event\"},{\"type\":11,\"id\":\"batch_read_function\"},{\"type\":11,\"id\":\"stream_read_function\"},{\"type\":11,\"id\":\"batch_count_function\"},{\"type\":11,\"id\":\"timestamp_to_stream_offset_function\"},{\"type\":11,\"id\":\"write_record_function\"},{\"type\":11,\"id\":\"query_by_advance_filter_function\"},{\"type\":11,\"id\":\"create_table_function\"},{\"type\":11,\"id\":\"clear_table_function\"},{\"type\":11,\"id\":\"drop_table_function\"},{\"type\":11,\"id\":\"create_index_function\"},{\"type\":11,\"id\":\"alter_field_attributes_function\"},{\"type\":11,\"id\":\"alter_field_name_function\"},{\"type\":11,\"id\":\"drop_field_function\"},{\"type\":11,\"id\":\"new_field_function\"},{\"type\":11,\"id\":\"get_table_names_function\"},{\"type\":20,\"id\":\"dml_insert_policy\",\"alternatives\":[\"update_on_exists\",\"ignore_on_exists\"]},{\"type\":20,\"id\":\"dml_update_policy\",\"alternatives\":[\"ignore_on_nonexists\",\"insert_on_nonexists\"]},{\"type\":20,\"id\":\"api_server_supported\"}]}}]},\"shareCache\":false,\"canOpenInspect\":false,\"isAutoInspect\":false,\"creator\":\"admin@admin.com\",\"showInspectTips\":false,\"taskRecordId\":\"637702eb6ed38f19015400c5\",\"alarmSettings\":[{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_STATUS_ERROR\",\"sort\":1,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_INSPECT_ERROR\",\"sort\":2,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_FULL_COMPLETE\",\"sort\":3,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_INCREMENT_START\",\"sort\":4,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_STATUS_STOP\",\"sort\":5,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_INCREMENT_DELAY\",\"sort\":6,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"}],\"alarmRules\":[{\"key\":\"TASK_INCREMENT_DELAY\",\"point\":3,\"equalsFlag\":1,\"ms\":500}],\"monitorStartDate\":\"2022-11-17T07:51:40.678+00:00\",\"autoInspect\":false,\"_deleted\":false,\"createTime\":\"2022-11-17T07:46:45.711+00:00\",\"last_updated\":\"2022-11-18T03:58:35.791+00:00\",\"user_id\":\"62bc5008d4958d013d97c7a6\"}";



    protected void initTask(Pair<String, Object>... kvs) {
        initTask(true, kvs);
    }
    protected void initTask(boolean castTaskId, Pair<String, Object>... kvs) {

        user = userService.loadUserById(MongoUtils.toObjectId("62bc5008d4958d013d97c7a6"));

        TaskDto taskDto = JsonUtil.parseJsonUseJackson(initJson, TaskDto.class);
        TaskEntity taskEntity = new TaskEntity();
        BeanUtils.copyProperties(taskDto, taskEntity);
        taskEntity.setId(null);
        taskEntity.setLastUpdAt(new Date());
        taskEntity.setStatus(TaskDto.STATUS_SCHEDULING);
        if (kvs != null && kvs.length > 0) {
            for (Pair<String, Object> kv : kvs) {
                for (Field field : taskEntity.getClass().getDeclaredFields()) {
                    if (field.getName().equals(kv.getKey())) {
                        try {
                            field.setAccessible(true);
                            field.set(taskEntity, kv.getValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }

        TaskEntity save = taskRepository.save(taskEntity, user);
        if (castTaskId) {
            taskId = save.getId();
        }

    }


    protected void initWorker(Pair<String, Object>... kvs) {
        WorkerDto workerDto = new WorkerDto();
        workerDto.setProcessId("zed_flow_engine");
        workerDto.setWorkerType("connector");
        workerDto.setWeight(1);
        workerDto.setPingTime(System.currentTimeMillis());
        workerDto.setRunningThread(0);
        workerDto.setTotalThread(1);
        workerDto.setUsedMemory(973078528L);
        workerDto.setVersion("-");
        workerDto.setAccessCode("asds-dfsdfs-dfsdf");
        workerDto.setStopping(false);
        workerDto.setIsDeleted(false);
        workerDto.setDeleted(false);
        workerDto.setStartTime(System.currentTimeMillis());
        workerDto.setUserId("62bc5008d4958d013d97c7a6");
        workerDto.setCreateUser("62bc5008d4958d013d97c7a6");

        if (kvs != null && kvs.length > 0) {
            for (Pair<String, Object> kv : kvs) {
                for (Field field : workerDto.getClass().getDeclaredFields()) {
                    if (field.getName().equals(kv.getKey())) {
                        try {
                            field.setAccessible(true);
                            field.set(workerDto, kv.getValue());
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }

        workerService.save(workerDto, user);
    }





    @Test
    public void manuallySpecifiedSucc() {
        //构造一个任务对象。指定了agentId, 并且能找到指定agent的情况
        taskRepository.deleteAll(new Query());
        initTask(ImmutablePair.of("accessNodeType", AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name())
                , ImmutablePair.of("accessNodeProcessId", "zed_flow_engine")
                , ImmutablePair.of("accessNodeProcessIdList", Lists.of("zed_flow_engine")));
        workerService.deleteAll(new Query());
        initWorker();
        Mockito.doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
        TaskDto taskDto = taskService.findById(taskId);
        taskScheduleService.scheduling(taskDto, user);
        assertEquals(TaskDto.STATUS_WAIT_RUN, taskDto.getStatus());
    }

    @Test
    public void manuallySpecifiedFail() {
        //构造一个任务对象。指定了agentId, 并且找不到指定agent的情况
        taskRepository.deleteAll(new Query());
        initTask(ImmutablePair.of("accessNodeType", AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name())
                , ImmutablePair.of("accessNodeProcessId", "zed_flow_engine1")
                , ImmutablePair.of("accessNodeProcessIdList", Lists.of("zed_flow_engine1")));
        workerService.deleteAll(new Query());
        initWorker();
        Mockito.doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
        TaskDto taskDto = taskService.findById(taskId);
        try {
            taskScheduleService.scheduling(taskDto, user);
        } catch (BizException e) {
            assertEquals("Task.AgentNotFound", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULE_FAILED, taskDto.getStatus());
    }

    @Test
    public void agentNotFound() {
        //找不到可用agent
        taskRepository.deleteAll(new Query());
        initTask();
        workerService.deleteAll(new Query());
        Mockito.doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
        TaskDto taskDto = taskService.findById(taskId);
        try {
            taskScheduleService.scheduling(taskDto, user);
        } catch (BizException e) {
            assertEquals("Task.AgentNotFound", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULE_FAILED, taskDto.getStatus());

    }


    @Test
    public void agentNormal() {
        //找不到可用pingTime不超时的agent
        taskRepository.deleteAll(new Query());
        initTask();
        workerService.deleteAll(new Query());
        initWorker();
        Mockito.doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
        TaskDto taskDto = taskService.findById(taskId);
            taskScheduleService.scheduling(taskDto, user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_WAIT_RUN, taskDto.getStatus());
    }

    @Test
    public void agentPingTimeout() {
        //找不到可用pingTime不超时的agent
        taskRepository.deleteAll(new Query());
        initTask();
        workerService.deleteAll(new Query());
        initWorker(ImmutablePair.of("pingTime", System.currentTimeMillis() - 500000));
        Mockito.doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
        TaskDto taskDto = taskService.findById(taskId);
        try {
            taskScheduleService.scheduling(taskDto, user);
        } catch (BizException e) {
            assertEquals("Task.AgentNotFound", e.getErrorCode());
        }
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_SCHEDULE_FAILED, taskDto.getStatus());
    }

    @Test
    public void moreFreeAgent() {
        //找到多个空闲agent，取权重

        //找不到可用pingTime不超时的agent
        taskRepository.deleteAll(new Query());
        initTask();
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed01"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed01"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed01"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed02"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed02"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed02"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed02"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed02"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed03"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed03"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed04"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed04"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed04"));
        initTask(false, ImmutablePair.of("status", TaskDto.STATUS_RUNNING), ImmutablePair.of("agentId", "zed04"));
        workerService.deleteAll(new Query());
        initWorker(ImmutablePair.of("processId", "zed01"), ImmutablePair.of("runningThread", 3));
        initWorker(ImmutablePair.of("processId", "zed02"), ImmutablePair.of("runningThread", 5));
        initWorker(ImmutablePair.of("processId", "zed03"), ImmutablePair.of("runningThread", 2));
        initWorker(ImmutablePair.of("processId", "zed04"), ImmutablePair.of("runningThread", 4));
        Mockito.doNothing().when(messageQueueService).sendMessage(any(MessageQueueDto.class));
        TaskDto taskDto = taskService.findById(taskId);
        taskScheduleService.scheduling(taskDto, user);
        taskDto = taskService.findById(taskId);
        assertEquals(TaskDto.STATUS_WAIT_RUN, taskDto.getStatus());
        assertEquals("zed03", taskDto.getAgentId());
    }
}
