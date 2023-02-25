package com.tapdata.tm.taskstatus;


import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.TMApplication;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;

import java.lang.reflect.Field;



@ActiveProfiles("dev")
@SpringBootTest(classes = {TMApplication.class})
public class TaskStatusTests {

    @Autowired
    protected TaskRepository taskRepository;

    @Autowired
    private UserService userService;

    protected UserDetail user;

    protected ObjectId taskId;


    private String initJson = "{\"id\":\"6375e6e589668816de276a5d\",\"lastUpdBy\":\"62bc5008d4958d013d97c7a6\",\"createUser\":\"admin@admin.com\",\"attrs\":{},\"deduplicWriteMode\":\"intelligent\",\"desc\":\"\",\"increHysteresis\":false,\"increOperationMode\":false,\"increSyncConcurrency\":true,\"processorThreadNum\":1,\"increaseReadSize\":1,\"readBatchSize\":500,\"isAutoCreateIndex\":true,\"isFilter\":false,\"isOpenAutoDDL\":false,\"isSchedule\":false,\"isStopOnError\":true,\"name\":\"新任务@15:46:45\",\"shareCdcEnable\":false,\"statuses\":[],\"status\":\"wait_start\",\"type\":\"initial_sync+cdc\",\"writeThreadSize\":8,\"editVersion\":\"1668681194984\",\"syncPoints\":[{}],\"syncType\":\"sync\",\"transformProcess\":0.0,\"planStartDateFlag\":false,\"accessNodeType\":\"AUTOMATIC_PLATFORM_ALLOCATION\",\"accessNodeProcessIdList\":[],\"accessNodeProcessId\":\"\",\"isEdit\":true,\"scheduledTime\":\"2022-11-17T07:51:40.683+00:00\",\"stoppingTime\":\"2022-11-17T10:22:43.286+00:00\",\"runningTime\":\"2022-11-17T07:51:41.470+00:00\",\"pingTime\":1668680571596,\"restartFlag\":false,\"transformUuid\":\"1668681194924\",\"transformed\":true,\"transformDagHash\":0,\"canForceStopping\":true,\"dag\":{\"edges\":[{\"source\":\"8326bc9a-ae30-4c75-9c17-c27461152977\",\"target\":\"861f38dc-cede-487b-a40a-74f21afe6438\"}],\"nodes\":[{\"tableName\":\"xcxcxczv\",\"isFilter\":true,\"increaseReadSize\":500,\"readBatchSize\":500,\"maxTransactionDuration\":12.0,\"existDataProcessMode\":\"keepData\",\"writeStrategy\":\"updateOrInsert\",\"updateConditionFields\":[\"COLUMN1\"],\"xmlIncludeFile\":false,\"esFragmentNum\":3,\"nodeConfig\":{},\"connectionId\":\"62d7e68b5fcbb347ae36d0b7\",\"databaseType\":\"MongoDB\",\"initialConcurrent\":true,\"initialConcurrentWriteNum\":8,\"type\":\"table\",\"catalog\":\"data\",\"isTransformed\":false,\"alarmSettings\":[{\"type\":\"DATANODE\",\"open\":true,\"key\":\"DATANODE_CANNOT_CONNECT\",\"sort\":1,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"DATANODE\",\"open\":true,\"key\":\"DATANODE_AVERAGE_HANDLE_CONSUME\",\"sort\":4,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"}],\"alarmRules\":[{\"key\":\"DATANODE_AVERAGE_HANDLE_CONSUME\",\"point\":3,\"equalsFlag\":1,\"ms\":500}],\"id\":\"861f38dc-cede-487b-a40a-74f21afe6438\",\"name\":\"xcxcxczv\",\"elementType\":\"Node\",\"attrs\":{\"position\":[60,385],\"connectionName\":\"zed_mongo\",\"connectionType\":\"source_and_target\",\"accessNodeProcessId\":\"\",\"pdkType\":\"pdk\",\"pdkHash\":\"4335aaa005ec1a74a4e2166bded2962e939ad50239f48b023b884f35b54129a5\",\"capabilities\":[{\"type\":11,\"id\":\"batch_read_function\"},{\"type\":11,\"id\":\"stream_read_function\"},{\"type\":11,\"id\":\"batch_count_function\"},{\"type\":11,\"id\":\"timestamp_to_stream_offset_function\"},{\"type\":11,\"id\":\"write_record_function\"},{\"type\":11,\"id\":\"query_by_advance_filter_function\"},{\"type\":11,\"id\":\"drop_table_function\"},{\"type\":11,\"id\":\"create_index_function\"},{\"type\":11,\"id\":\"get_table_names_function\"},{\"type\":11,\"id\":\"error_handle_function\"},{\"type\":20,\"id\":\"master_slave_merge\"},{\"type\":20,\"id\":\"dml_insert_policy\",\"alternatives\":[\"update_on_exists\",\"ignore_on_exists\"]},{\"type\":20,\"id\":\"dml_update_policy\",\"alternatives\":[\"ignore_on_nonexists\",\"insert_on_nonexists\"]},{\"type\":20,\"id\":\"api_server_supported\"}]}},{\"tableName\":\"04120001\",\"isFilter\":true,\"increaseReadSize\":500,\"readBatchSize\":500,\"maxTransactionDuration\":12.0,\"existDataProcessMode\":\"keepData\",\"writeStrategy\":\"updateOrInsert\",\"xmlIncludeFile\":false,\"esFragmentNum\":3,\"connectionId\":\"629053b06436fc5fa5412547\",\"databaseType\":\"Mysql\",\"type\":\"table\",\"catalog\":\"data\",\"isTransformed\":false,\"alarmSettings\":[{\"type\":\"DATANODE\",\"open\":true,\"key\":\"DATANODE_CANNOT_CONNECT\",\"sort\":1,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"DATANODE\",\"open\":true,\"key\":\"DATANODE_AVERAGE_HANDLE_CONSUME\",\"sort\":4,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"}],\"alarmRules\":[{\"key\":\"DATANODE_AVERAGE_HANDLE_CONSUME\",\"point\":3,\"equalsFlag\":1,\"ms\":500}],\"id\":\"8326bc9a-ae30-4c75-9c17-c27461152977\",\"name\":\"04120001\",\"elementType\":\"Node\",\"attrs\":{\"position\":[-338,175],\"connectionName\":\"MySQL-33061\",\"connectionType\":\"source_and_target\",\"accessNodeProcessId\":\"\",\"pdkType\":\"pdk\",\"pdkHash\":\"a5af410b12afca476edf4a650c133ddf135bf76542a67787ed6f7f7d53ba712\",\"capabilities\":[{\"type\":10,\"id\":\"new_field_event\"},{\"type\":10,\"id\":\"alter_field_name_event\"},{\"type\":10,\"id\":\"alter_field_attributes_event\"},{\"type\":10,\"id\":\"drop_field_event\"},{\"type\":11,\"id\":\"batch_read_function\"},{\"type\":11,\"id\":\"stream_read_function\"},{\"type\":11,\"id\":\"batch_count_function\"},{\"type\":11,\"id\":\"timestamp_to_stream_offset_function\"},{\"type\":11,\"id\":\"write_record_function\"},{\"type\":11,\"id\":\"query_by_advance_filter_function\"},{\"type\":11,\"id\":\"create_table_function\"},{\"type\":11,\"id\":\"clear_table_function\"},{\"type\":11,\"id\":\"drop_table_function\"},{\"type\":11,\"id\":\"create_index_function\"},{\"type\":11,\"id\":\"alter_field_attributes_function\"},{\"type\":11,\"id\":\"alter_field_name_function\"},{\"type\":11,\"id\":\"drop_field_function\"},{\"type\":11,\"id\":\"new_field_function\"},{\"type\":11,\"id\":\"get_table_names_function\"},{\"type\":20,\"id\":\"dml_insert_policy\",\"alternatives\":[\"update_on_exists\",\"ignore_on_exists\"]},{\"type\":20,\"id\":\"dml_update_policy\",\"alternatives\":[\"ignore_on_nonexists\",\"insert_on_nonexists\"]},{\"type\":20,\"id\":\"api_server_supported\"}]}}]},\"shareCache\":false,\"canOpenInspect\":false,\"isAutoInspect\":false,\"creator\":\"admin@admin.com\",\"showInspectTips\":false,\"taskRecordId\":\"637702eb6ed38f19015400c5\",\"alarmSettings\":[{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_STATUS_ERROR\",\"sort\":1,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_INSPECT_ERROR\",\"sort\":2,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_FULL_COMPLETE\",\"sort\":3,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_INCREMENT_START\",\"sort\":4,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_STATUS_STOP\",\"sort\":5,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"},{\"type\":\"TASK\",\"open\":true,\"key\":\"TASK_INCREMENT_DELAY\",\"sort\":6,\"notify\":[\"SYSTEM\",\"EMAIL\"],\"interval\":500,\"unit\":\"MS\"}],\"alarmRules\":[{\"key\":\"TASK_INCREMENT_DELAY\",\"point\":3,\"equalsFlag\":1,\"ms\":500}],\"monitorStartDate\":\"2022-11-17T07:51:40.678+00:00\",\"autoInspect\":false,\"_deleted\":false,\"createTime\":\"2022-11-17T07:46:45.711+00:00\",\"last_updated\":\"2022-11-18T03:58:35.791+00:00\",\"user_id\":\"62bc5008d4958d013d97c7a6\"}";



    protected void initTask(String status, Pair<String, Object>... kvs) {

        taskRepository.deleteAll(new Query());

        user = userService.loadUserById(MongoUtils.toObjectId("62bc5008d4958d013d97c7a6"));

        TaskDto taskDto = JsonUtil.parseJsonUseJackson(initJson, TaskDto.class);
        TaskEntity taskEntity = new TaskEntity();
        BeanUtils.copyProperties(taskDto, taskEntity);
        taskEntity.setId(null);
        taskEntity.setStatus(status);
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
        taskId = save.getId();

    }
}
