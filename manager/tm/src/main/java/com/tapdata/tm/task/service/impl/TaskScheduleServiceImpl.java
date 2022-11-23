package com.tapdata.tm.task.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.autoinspect.service.TaskAutoInspectResultsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.handler.ExceptionHandler;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.DataSyncMq;
import com.tapdata.tm.commons.task.dto.TaskCollectionObjDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.disruptor.constants.DisruptorTopicEnum;
import com.tapdata.tm.disruptor.service.DisruptorService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetaDataHistoryService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.service.*;
import com.tapdata.tm.transform.service.MetadataTransformerItemService;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Map;
import java.util.Objects;


@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskScheduleServiceImpl implements TaskScheduleService {
    private WorkerService workerService;
    private TaskService taskService;
    private MessageQueueService messageQueueService;
    private DisruptorService disruptorService;
    private MonitoringLogsService monitoringLogsService;
    private TaskCollectionObjService taskCollectionObjService;

    private StateMachineService stateMachineService;

    @Override
    public void scheduling(TaskDto taskDto, UserDetail user) {

        if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), taskDto.getAccessNodeType())
                && CollectionUtils.isNotEmpty(taskDto.getAccessNodeProcessIdList())) {
            taskDto.setAgentId(taskDto.getAccessNodeProcessIdList().get(0));
        } else {
            taskDto.setAgentId(null);
        }

        CalculationEngineVo calculationEngineVo = workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName());
        if (StringUtils.isBlank(taskDto.getAgentId())) {
            scheduleFailed(taskDto, user);
        }

        Date now = new Date();
        monitoringLogsService.agentAssignMonitoringLog(taskDto, calculationEngineVo.getProcessId(), calculationEngineVo.getAvailable(), user, now);
        //调度完成之后，改成待运行状态
        Query query1 = new Query(Criteria.where("_id").is(taskDto.getId()));
        Update waitRunUpdate = Update.update("agentId", taskDto.getAgentId())
                .set("monitorStartDate", now)
                .set("last_updated", now);
        boolean needCreateRecord = false;
        if (StringUtils.isBlank(taskDto.getTaskRecordId())) {
            taskDto.setTaskRecordId(new ObjectId().toHexString());
            waitRunUpdate.set(TaskDto.LASTTASKRECORDID, taskDto.getTaskRecordId());
            needCreateRecord = true;
        }

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_SUCCESS, user);

        if (stateMachineResult.isFail()) {
            log.info("concurrent start operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return;
        } else {

            UpdateResult waitRunResult = taskService.update(query1, waitRunUpdate, user);
            taskService.updateTaskRecordStatus(taskDto, TaskDto.STATUS_WAIT_RUN, user);
        }
        //发送websocket消息，提醒flowengin启动
        DataSyncMq dataSyncMq = new DataSyncMq();
        dataSyncMq.setTaskId(taskDto.getId().toHexString());
        dataSyncMq.setOpType(DataSyncMq.OP_TYPE_START);
        dataSyncMq.setType(MessageType.DATA_SYNC.getType());

        Map<String, Object> data;
        String json = JsonUtil.toJsonUseJackson(dataSyncMq);
        data = JsonUtil.parseJsonUseJackson(json, Map.class);
        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(taskDto.getAgentId());
        queueDto.setData(data);
        queueDto.setType("pipe");

        log.debug("build start task websocket context, processId = {}, userId = {}, queueDto = {}", taskDto.getAgentId(), user.getUserId(), queueDto);
        messageQueueService.sendMessage(queueDto);

        if (needCreateRecord) {
            TaskEntity taskSnapshot = new TaskEntity();
            BeanUtil.copyProperties(taskDto, taskSnapshot);
            disruptorService.sendMessage(DisruptorTopicEnum.CREATE_RECORD, new TaskRecord(taskDto.getTaskRecordId(), taskDto.getId().toHexString(), taskSnapshot, user.getUserId(), now));
        } else {
            taskService.updateTaskRecordStatus(taskDto, taskDto.getStatus(), user);
        }

        //数据发现的任务收集
        TaskDto newTaskDto = taskService.findById(taskDto.getId(), user);
        TaskCollectionObjDto taskCollectionObjDto = new TaskCollectionObjDto();
        BeanUtils.copyProperties(newTaskDto, taskCollectionObjDto);
        Query query2 = new Query(Criteria.where("_id").is(taskDto.getId()));
        taskCollectionObjService.upsert(query2, taskCollectionObjDto, user);

        if (Objects.isNull(newTaskDto.getScheduleDate())) {
            taskService.update(Query.query(Criteria.where("_id").is(taskDto.getId())),
                    Update.update("scheduleDate", System.currentTimeMillis()));
        }
    }

    /**
     * @see DataFlowEvent#SCHEDULE_FAILED
     * @param taskDto
     * @param user
     */
    public void scheduleFailed(TaskDto taskDto, UserDetail user) {
        log.warn("No available agent found, task name = {}", taskDto.getName());
        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user);
        throw new BizException("Task.AgentNotFound");
    }

}
