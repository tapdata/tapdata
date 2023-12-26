package com.tapdata.tm.disruptor.handler;

import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.google.common.collect.Maps;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.disruptor.Element;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component("updateRecordStatusEventHandler")
public class UpdateRecordStatusEventHandler implements BaseEventHandler<SyncTaskStatusDto, Boolean>{

    private static final String TAG = UpdateRecordStatusEventHandler.class.getSimpleName();
    public static final String SHARE_CDC_TASK_STOP_WARNING_KEY = "share_cdc_task_stop_warning";
    @Autowired
    private TaskService taskService;

    @Override
    public Boolean onEvent(Element<SyncTaskStatusDto> event, long sequence, boolean endOfBatch) {

        TaskRecordService taskRecordService = SpringUtil.getBean(TaskRecordService.class);
        SyncTaskStatusDto data = event.getData();
        taskRecordService.updateTaskStatus(data);
        CompletableFuture.runAsync(() -> taskAlarm(data));
        CompletableFuture.runAsync(()->logCollectorAlarm(data));

        return true;
    }

    private void taskAlarm(SyncTaskStatusDto data) {
        AlarmService alarmService = SpringUtil.getBean(AlarmService.class);

        String taskId = data.getTaskId();
        String taskName = data.getTaskName();
        String alarmDate = DateUtil.now();
        CommonUtils.ignoreAnyError(()->Thread.currentThread().setName(String.format("task-status-alarm-%s(%s)-%s", taskName, taskId, data.getTaskStatus())), TAG);
        Map<String, Object> param = Maps.newHashMap();
        switch (data.getTaskStatus()) {
            case TaskDto.STATUS_RUNNING:
                alarmService.closeWhenTaskRunning(taskId);

                break;
            case TaskDto.STATUS_ERROR:
                TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
                boolean checkOpen = alarmService.checkOpen(taskDto, null, AlarmKeyEnum.TASK_STATUS_ERROR, null, data.getUserDetail());
                if (checkOpen) {
                    param.put("taskName", taskName);
                    param.put("alarmDate", alarmDate);
                    AlarmInfo errorInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.EMERGENCY).component(AlarmComponentEnum.FE)
                            .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(data.getAgentId()).taskId(taskId)
                            .name(data.getTaskName()).summary("TASK_STATUS_STOP_ERROR").metric(AlarmKeyEnum.TASK_STATUS_ERROR)
                            .param(param)
                            .build();
                    errorInfo.setUserId(taskDto.getUserId());
                    alarmService.save(errorInfo);
                }
                break;
            case TaskDto.STATUS_STOP:
                TaskDto stopTaskDto = taskService.findById(MongoUtils.toObjectId(taskId));
                boolean checkOpenForStop = alarmService.checkOpen(stopTaskDto, null, AlarmKeyEnum.TASK_STATUS_STOP, null, data.getUserDetail());
                if (checkOpenForStop) {
                    param.put("taskName", taskName);
                    param.put("stopTime", DateUtil.now());
                    param.put("alarmDate", alarmDate);
                    AlarmInfo errorInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.NORMAL).component(AlarmComponentEnum.FE)
                            .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(data.getAgentId()).taskId(taskId)
                            .name(data.getTaskName()).summary("TASK_STATUS_STOP").metric(AlarmKeyEnum.TASK_STATUS_STOP)
                            .param(param)
                            .build();
                    errorInfo.setUserId(stopTaskDto.getUserId());
                    alarmService.save(errorInfo);
                }
                break;
        }
    }

    private void logCollectorAlarm(SyncTaskStatusDto data) {
        String taskId = data.getTaskId();
        String taskName = data.getTaskName();
        String taskStatus = data.getTaskStatus();
        CommonUtils.ignoreAnyError(() -> Thread.currentThread().setName(String.format("log-collector-task-alarm-%s(%s)-%s", taskName, taskId, taskStatus)), TAG);
        Field field = new Field();
        field.put("name", true);
        field.put("syncType", true);
        field.put("status", true);
        field.put("dag", true);
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId), field);
        if (null == taskDto) {
            return;
        }
        Set<String> queryConnIds = new HashSet<>();
        List<Node> sourceNodes = taskDto.getDag().getSourceNodes();
        if (CollectionUtils.isEmpty(sourceNodes)) {
            return;
        }
        for (Node sourceNode : sourceNodes) {
            if (!(sourceNode instanceof LogCollectorNode)) {
                continue;
            }
            LogCollectorNode logCollectorNode = (LogCollectorNode) sourceNode;
            // 旧版本连接id
            List<String> connectionIds = logCollectorNode.getConnectionIds();
            if(CollectionUtils.isNotEmpty(connectionIds)) {
                queryConnIds.addAll(connectionIds);
            }
            // 新版本连接id
            Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
            if (MapUtils.isNotEmpty(logCollectorConnConfigs)) {
                queryConnIds.addAll(logCollectorConnConfigs.keySet());
            }
        }

        if (CollectionUtils.isEmpty(queryConnIds)) {
            return;
        }
        List<Criteria> orList = new ArrayList<>();
        for (String queryConnId : queryConnIds) {
            orList.add(Criteria.where("shareCdcTaskId."+queryConnId).is(taskId));
        }
        Query query = Query.query(new Criteria().orOperator(orList));
        Update update = null;
        switch (taskStatus) {
            case TaskDto.STATUS_ERROR:
            case TaskDto.STATUS_STOP:
                update = new Update()
                        .set("shareCdcStop", true)
                        .set("shareCdcStopMessage", SHARE_CDC_TASK_STOP_WARNING_KEY);
                break;
            case TaskDto.STATUS_WAIT_RUN:
            case TaskDto.STATUS_RUNNING:
                update = new Update().set("shareCdcStop", false).set("shareCdcStopMessage", "");
                break;
            default:
                break;
        }

        if(null != update) {
            System.out.println(query.getQueryObject().toJson());
            System.out.println(update.getUpdateObject().toJson());
            UpdateResult updateResult = taskService.updateMany(query, update);
            System.out.println(updateResult);
        }
    }
}
