package com.tapdata.tm.task.service.impl;

import com.alibaba.fastjson.JSON;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.DataSyncMq;
import com.tapdata.tm.commons.task.dto.TaskCollectionObjDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.disruptor.service.DisruptorService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskCollectionObjService;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


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
    private SettingsService settingsService;
    private StateMachineService stateMachineService;

    @Override
    public void scheduling(TaskDto taskDto, UserDetail user) {

        AtomicBoolean needCalculateAgent = new AtomicBoolean(true);
        String agentId = taskDto.getAgentId();
        Optional.ofNullable(agentId).ifPresent(id -> {
            List<Worker> workerList = workerService.findAvailableAgentByAccessNode(user, Lists.newArrayList(agentId));
            if (CollectionUtils.isNotEmpty(workerList)) {
                Worker workerDto = workerList.get(0);

                Object heartTime = settingsService.getValueByCategoryAndKey(CategoryEnum.WORKER, KeyEnum.WORKER_HEART_TIMEOUT);
                long heartExpire = Objects.nonNull(heartTime) ? (Long.parseLong(heartTime.toString()) + 48) * 1000 : 108000;

                if (workerDto.getPingTime() < heartExpire) {
                    needCalculateAgent.set(false);
                }
            }
        });

        if (needCalculateAgent.get()) {
            if (AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name().equals(taskDto.getAccessNodeType())
                    && CollectionUtils.isNotEmpty(taskDto.getAccessNodeProcessIdList())) {
                taskDto.setAgentId(taskDto.getAccessNodeProcessIdList().get(0));
            } else {
                taskDto.setAgentId(null);
            }
        }

        if (AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name().equals(taskDto.getAccessNodeType())
                && CollectionUtils.isNotEmpty(taskDto.getAccessNodeProcessIdList())) {
            int num = taskService.runningTaskNum(taskDto.getAgentId(), user);
            WorkerDto workerDto = workerService.findByProcessId(taskDto.getAgentId(), user, "agentTags");
            int limitTaskNum = workerDto.getLimitTaskNum();
            if (limitTaskNum <= num) {
                StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user);
                if (stateMachineResult.isOk()) {
                    throw new BizException("Task.ScheduleLimit");
                }
            }
        }

        CalculationEngineVo calculationEngineVo = workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName());

        if (StringUtils.isBlank(taskDto.getAgentId()) && calculationEngineVo.getTaskAvailable() != calculationEngineVo.getAvailable()) {
            StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user);
            if (stateMachineResult.isOk()) {
                throw new BizException("Task.ScheduleLimit");
            }
        }

        FunctionUtils.ignoreAnyError(() -> {
            String template = "Scheduling calculation results: {0}, all agent data: {1}.";
            String msg = MessageFormat.format(template, calculationEngineVo.getProcessId() , JSON.toJSONString(calculationEngineVo.getThreadLog()));
            monitoringLogsService.startTaskErrorLog(taskDto, user, msg, Level.INFO);
        });
        if (StringUtils.isBlank(taskDto.getAgentId())) {
            scheduleFailed(taskDto, user);
        }

        Date now = new Date();
        monitoringLogsService.agentAssignMonitoringLog(taskDto, calculationEngineVo.getProcessId(), calculationEngineVo.getAvailable(), user, now);
        //调度完成之后，改成待运行状态
        Query query1 = new Query(Criteria.where("_id").is(taskDto.getId()));
        Update waitRunUpdate = Update.update("agentId", taskDto.getAgentId()).set("last_updated", now);

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_SUCCESS, user);

        if (stateMachineResult.isFail()) {
            log.info("concurrent start operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return;
        } else {
            taskService.update(query1, waitRunUpdate, user);
        }
        sendStartMsg(taskDto.getId().toHexString(), taskDto.getAgentId(), user);

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

    public void sendStartMsg(String taskId, String agentId, UserDetail user) {
        //发送websocket消息，提醒flowengin启动
        DataSyncMq dataSyncMq = new DataSyncMq();
        dataSyncMq.setTaskId(taskId);
        dataSyncMq.setOpType(DataSyncMq.OP_TYPE_START);
        dataSyncMq.setType(MessageType.DATA_SYNC.getType());

        Map<String, Object> data;
        String json = JsonUtil.toJsonUseJackson(dataSyncMq);
        data = JsonUtil.parseJsonUseJackson(json, Map.class);
        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(agentId);
        queueDto.setData(data);
        queueDto.setType("pipe");

        log.debug("build start task websocket context, processId = {}, userId = {}, queueDto = {}", agentId, user.getUserId(), queueDto);
        messageQueueService.sendMessage(queueDto);
    }

    /**
     * @see DataFlowEvent#SCHEDULE_FAILED
     * @param taskDto
     * @param user
     */
    public void scheduleFailed(TaskDto taskDto, UserDetail user) {
        log.warn("No available agent found, task name = {}", taskDto.getName());
        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user);
        if (AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name().equals(taskDto.getAccessNodeType())
                && CollectionUtils.isNotEmpty(taskDto.getAccessNodeProcessIdList())) {
            throw new BizException("Task.SpecifyAgentOffline", taskDto.getAccessNodeProcessIdList().get(0));
        } else {
            throw new BizException("Task.AgentNotFound");
        }
    }

}
