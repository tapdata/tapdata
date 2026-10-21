package com.tapdata.tm.task.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.agent.util.AgentGroupTag;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.CheckTaskMemoryResult;
import com.tapdata.tm.commons.task.dto.DataSyncMq;
import com.tapdata.tm.commons.task.dto.TaskCollectionObjDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskCollectionObjService;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
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
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskScheduleServiceImpl implements TaskScheduleService {
    private WorkerService workerService;
    private TaskService taskService;
    private MessageQueueService messageQueueService;
    private MonitoringLogsService monitoringLogsService;
    private TaskCollectionObjService taskCollectionObjService;
    private SettingsService settingsService;
    private StateMachineService stateMachineService;
    private UserService userService;
    private AgentGroupService agentGroupService;
    private DataSourceService dataSourceService;

    private static final class ConnectionAgentPolicy {
        private final Set<String> allowedAgents;
        private final List<String> connectionNames;
        private final List<String> priorityAgents;

        private ConnectionAgentPolicy(Set<String> allowedAgents, List<String> connectionNames, List<String> priorityAgents) {
            this.allowedAgents = allowedAgents;
            this.connectionNames = connectionNames;
            this.priorityAgents = priorityAgents;
        }
    }

    @Override
    public void scheduling(TaskDto taskDto, UserDetail user,Boolean isReStart) {
        FunctionUtils.ignoreAnyError(() -> {
            CheckTaskMemoryResult checkTaskMemoryResult = taskService.checkTaskMemoryHeap(taskDto, isReStart, user);
            if (null != checkTaskMemoryResult && !checkTaskMemoryResult.getIsSafe()) {
                String message = String.format("Potential memory risk detected.The current task may require more resources than available.The current Batch Size is %s with an Avg Data Size of %s, but Available Memory is only %s."
                        , checkTaskMemoryResult.getBatchSize(), checkTaskMemoryResult.getAvgSize(), checkTaskMemoryResult.getRealFree());
                monitoringLogsService.startTaskErrorLog(taskDto, user, message, Level.WARN);
            }
        });
        CalculationEngineVo calculationEngineVo = cloudTaskLimitNum(taskDto, user, false);
        UserDetail finalUser = user;
        FunctionUtils.ignoreAnyError(() -> {
            String template = "Scheduling calculation results: {0}, all agent data: {1}.";
            String msg = MessageFormat.format(template, calculationEngineVo.getProcessId() , JSON.toJSONString(calculationEngineVo.getThreadLog()));
            monitoringLogsService.startTaskErrorLog(taskDto, finalUser, msg, Level.INFO);
        });
        // Fix: When no available agent is found, keep task in SCHEDULING state and wait for retry
        // instead of setting it to SCHEDULING_FAILED (error) state
        if (StringUtils.isBlank(taskDto.getAgentId())) {
            log.warn("No available agent found for task [{}], will keep in SCHEDULING state and retry in next round", taskDto.getName());
            CompletableFuture.runAsync(() -> {
                String template = "No available agent found, task will remain in scheduling state and retry automatically when agent becomes available.";
                monitoringLogsService.startTaskErrorLog(taskDto, finalUser, template, Level.WARN);
            });
            return; // Keep task in SCHEDULING state, will be retried by TaskRestartSchedule
        }

        Date now = new Date();
        monitoringLogsService.agentAssignMonitoringLog(taskDto, calculationEngineVo.getProcessId(), calculationEngineVo.getProcessName(), calculationEngineVo.getAvailable(), calculationEngineVo.isManually(), user, now);

        // Step 1: Write agentId first while still in SCHEDULING state.
        // This is safe because no scheduler dispatches start messages to tasks in SCHEDULING state.
        // Ensures any observer of the subsequent WAIT_RUN state always sees the correct agentId.
        Query agentIdQuery = new Query(Criteria.where("_id").is(taskDto.getId())
                .and("status").is(TaskDto.STATUS_SCHEDULING));
        Update waitRunUpdate = Update.update("agentId", taskDto.getAgentId()).set("last_updated", now);
        if(Boolean.TRUE.equals(taskDto.getCheckMemoryHeap())){
            waitRunUpdate.set("checkMemoryHeap", false);
        }
        taskService.update(agentIdQuery, waitRunUpdate, user);

        // Step 2: Transition state to WAIT_RUN — agentId is already correct at this point
        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_SUCCESS, user);

        if (stateMachineResult.isFail()) {
            log.info("concurrent start operations, this operation don’t effective, task name = {}", taskDto.getName());
            return;
        }
        sendStartMsg(taskDto.getId().toHexString(), taskDto.getAgentId(), user);

        //数据发现的任务收集
        TaskDto newTaskDto = taskService.findById(taskDto.getId(), user);
        TaskCollectionObjDto taskCollectionObjDto = new TaskCollectionObjDto();
        BeanUtils.copyProperties(newTaskDto, taskCollectionObjDto);
        Query query2 = new Query(Criteria.where("_id").is(taskDto.getId()));
        taskCollectionObjService.upsert(query2, taskCollectionObjDto);

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
        List<String> processNodeListWithGroup = agentGroupService.getProcessNodeListWithGroup(taskDto, user);
        if (AccessNodeTypeEnum.isManually(taskDto.getAccessNodeType())
                && CollectionUtils.isNotEmpty(processNodeListWithGroup)) {
            throw new BizException("Task.SpecifyAgentOffline", processNodeListWithGroup.get(0));
        } else {
            throw new BizException("Task.AgentNotFound");
        }
    }


    public CalculationEngineVo cloudTaskLimitNum(TaskDto taskDto, UserDetail user, boolean limitNum) {
        TaskDto userId = taskService.findByTaskId(taskDto.getId(), "user_id");
        Assert.notNull(userId, "task not found");
        user = userService.loadUserById(new ObjectId(userId.getUserId()));

        // Resolve the effective Agent policy only for this scheduling attempt. The task keeps
        // its own automatic/manual setting, while connection policies restrict the runtime
        // candidate set. This is shared by data development, data copy, shared mining and
        // connection heartbeat tasks.
        TaskDto schedulingTaskDto = taskForConnectionAgentPolicy(taskDto, user);
        if (schedulingTaskDto == null) {
            taskDto.setAgentId(null);
            return noAvailableAgentResult();
        }
        // A manual policy must always be evaluated from its configured Agent/group. Never let
        // the AgentId from a previous run bypass the task/connection policy check.
        if (AccessNodeTypeEnum.isManually(schedulingTaskDto.getAccessNodeType())) {
            schedulingTaskDto.setAgentId(null);
        }
        AtomicBoolean needCalculateAgent = new AtomicBoolean(true);
        String agentId = schedulingTaskDto.getAgentId();
        UserDetail finalUser = user;
        Optional.ofNullable(agentId).ifPresent(id -> {
            List<Worker> workerList = workerService.findAvailableAgentByAccessNode(finalUser, Lists.newArrayList(agentId));
            if (CollectionUtils.isNotEmpty(workerList)) {
                Worker workerDto = workerList.get(0);

                long heartExpire = (long) SettingsEnum.WORKER_HEART_OVERTIME.getIntValue(30) * 1000L;

                if ((System.currentTimeMillis() - workerDto.getPingTime()) < heartExpire || Boolean.TRUE.equals(schedulingTaskDto.getCheckMemoryHeap())){
                    needCalculateAgent.set(false);
                }
            }
        });
        List<String> accessNodeProcessIdList;
        try {
            accessNodeProcessIdList = agentGroupService.getProcessNodeListWithGroup(schedulingTaskDto, user);
        } catch (BizException e) {
            if (!AccessNodeTypeEnum.isGroupManually(schedulingTaskDto.getAccessNodeType()) || !isUnresolvableGroup(e)) {
                throw e;
            }
            taskDto.setAgentId(null);
            return noAvailableAgentResult();
        }
        if (AccessNodeTypeEnum.isGroupManually(schedulingTaskDto.getAccessNodeType())
                && CollectionUtils.isEmpty(accessNodeProcessIdList)) {
            taskDto.setAgentId(null);
            return noAvailableAgentResult();
        }
        if (needCalculateAgent.get()) {
            if (AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name().equals(schedulingTaskDto.getAccessNodeType())
                    && CollectionUtils.isNotEmpty(schedulingTaskDto.getAccessNodeProcessIdList())) {
                schedulingTaskDto.setAgentId(schedulingTaskDto.getAccessNodeProcessIdList().get(0));
            } else if(AccessNodeTypeEnum.isGroupManually(schedulingTaskDto.getAccessNodeType())
                    && CollectionUtils.isNotEmpty(accessNodeProcessIdList)){
                List<Worker> availableAgent = workerService.findAvailableAgentByAccessNode(user,accessNodeProcessIdList);
                if (CollectionUtils.isEmpty(availableAgent)) {
                    log.warn("No available agent found in specified agent group for task [{}]", taskDto.getName());
                    taskDto.setAgentId(null);
                    return noAvailableAgentResult();
                }
                List<String> processIds = availableAgent.stream().map(Worker::getProcessId).collect(Collectors.toList());
                String finalAgentId = null;
                if(StringUtils.isNotEmpty(schedulingTaskDto.getPriorityProcessId()) && processIds.contains(schedulingTaskDto.getPriorityProcessId())){
                    finalAgentId = schedulingTaskDto.getPriorityProcessId();
                }else{
                    finalAgentId = processIds.get(0);
                }
                schedulingTaskDto.setAgentId(finalAgentId);
            } else {
                // TAP-12917 Automatic platform allocation: clear agentId so calculationEngine()
                // evaluates ALL available agents instead of early-exiting with only the pre-assigned one
                log.info("Task [{}] with automatic platform allocation, clearing agentId to evaluate all available agents", schedulingTaskDto.getName());
                schedulingTaskDto.setAgentId(null);
            }
        }

        if (AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name().equals(schedulingTaskDto.getAccessNodeType())
                && CollectionUtils.isNotEmpty(schedulingTaskDto.getAccessNodeProcessIdList())) {
            WorkerDto workerDto = workerService.findByProcessId(schedulingTaskDto.getAgentId(), user, "user_id", "agentTags", "process_id");
            if (workerDto != null) {
                int limitTaskNum = workerService.getLimitTaskNum(workerDto, user);
                int runningNum = taskService.subCronOrPlanNum(schedulingTaskDto, taskService.runningTaskNum(schedulingTaskDto.getAgentId(), user));
                if (runningNum > limitTaskNum && !limitNum) {
                    StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user);
                    if (stateMachineResult.isOk()) {
                        throw new BizException("Task.ScheduleLimit");
                    }
                }
            }
        }

        boolean strictTaskAgent = AccessNodeTypeEnum.isManually(schedulingTaskDto.getAccessNodeType());
        CalculationEngineVo calculationEngineVo;
        try {
            calculationEngineVo = strictTaskAgent
                    ? workerService.scheduleTaskToEngineWithStrictAgent(schedulingTaskDto, user, "task", schedulingTaskDto.getName())
                    : workerService.scheduleTaskToEngine(schedulingTaskDto, user, "task", schedulingTaskDto.getName());
        } catch (BizException e) {
            if (!strictTaskAgent || !"Task.AgentNotFound".equals(e.getErrorCode())) {
                throw e;
            }
            log.warn("Task Agent is unavailable for task [{}], waiting for the next scheduling round", taskDto.getName());
            taskDto.setAgentId(null);
            return noAvailableAgentResult();
        }
        if (schedulingTaskDto != taskDto) {
            // TAP-12917 Only the runtime result is copied back. accessNodeType/accessNodeProcessId
            // on the persisted task remain automatic/empty by design.
            taskDto.setAgentId(calculationEngineVo.getProcessId());
            taskDto.setScheduleTime(schedulingTaskDto.getScheduleTime());
        }
        int runningNum = taskService.subCronOrPlanNum(schedulingTaskDto, calculationEngineVo.getRunningNum());
        if (StringUtils.isNotBlank(schedulingTaskDto.getAgentId()) && runningNum > calculationEngineVo.getTaskLimit()
                && !limitNum) {
            StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user);
            if (stateMachineResult.isOk()) {
                throw new BizException("Task.ScheduleLimit");
            }
        }
        return calculationEngineVo;
    }

    @Override
    public void validateConnectionAgentPolicy(TaskDto taskDto, UserDetail user) {
        resolveEffectiveAgents(taskDto, resolveConnectionAgentPolicy(taskDto, user), user);
    }

    private Set<String> resolveEffectiveAgents(TaskDto taskDto, ConnectionAgentPolicy policy, UserDetail user) {
        if (policy == null || policy.allowedAgents == null) {
            return null;
        }
        if (CollectionUtils.isEmpty(policy.allowedAgents)) {
            throw new BizException("Task.SourceAgentConflict", String.join(", ", policy.connectionNames));
        }
        Set<String> effectiveAgents = new LinkedHashSet<>(policy.allowedAgents);
        if (hasTaskAgentPolicyOverride(taskDto)) {
            effectiveAgents.retainAll(taskAllowedAgents(taskDto, user));
            if (effectiveAgents.isEmpty()) {
                throw new BizException("Task.AgentConflict");
            }
        }
        return effectiveAgents;
    }

    private CalculationEngineVo noAvailableAgentResult() {
        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        calculationEngineVo.setAvailable(0);
        calculationEngineVo.setRunningNum(0);
        calculationEngineVo.setTaskLimit(Integer.MAX_VALUE);
        calculationEngineVo.setTotalLimit(Integer.MAX_VALUE);
        calculationEngineVo.setThreadLog(new ArrayList<>());
        calculationEngineVo.setManually(true);
        return calculationEngineVo;
    }

    private TaskDto taskForConnectionAgentPolicy(TaskDto taskDto, UserDetail user) {
        ConnectionAgentPolicy policy = resolveConnectionAgentPolicy(taskDto, user);
        Set<String> effectiveAgents = resolveEffectiveAgents(taskDto, policy, user);
        if (effectiveAgents == null) {
            return taskDto;
        }
        return runtimeTaskForAgentSet(taskDto, effectiveAgents, policy.priorityAgents, user);
    }

    private ConnectionAgentPolicy resolveConnectionAgentPolicy(TaskDto taskDto, UserDetail user) {
        if (!taskDto.usesConnectionAgentPolicy() || Boolean.TRUE.equals(taskDto.getShareCache()) || dataSourceService == null) {
            return null;
        }
        List<String> connectionIds = taskConnectionIds(taskDto);
        if (CollectionUtils.isEmpty(connectionIds)) {
            return null;
        }

        // Connections may belong to a different user than the task. Preserve the unscoped
        // ID lookup, but read only policy fields and resolve groups under their own owners.
        Query policyQuery = Query.query(Criteria.where("_id").in(connectionIds));
        policyQuery.fields().include("_id", "name", "accessNodeType", "accessNodeProcessId", "priorityProcessId", "user_id");
        List<DataSourceConnectionDto> connections = dataSourceService.findAll(policyQuery);
        if (CollectionUtils.isEmpty(connections)) {
            return null;
        }
        Map<String, DataSourceConnectionDto> byId = connections.stream()
                .filter(connection -> connection != null && connection.getId() != null)
                .collect(Collectors.toMap(connection -> connection.getId().toHexString(), connection -> connection, (first, second) -> first));
        Set<String> allowedAgentIntersection = null;
        List<String> restrictedConnectionNames = new ArrayList<>();
        List<String> priorityAgents = new ArrayList<>();
        Map<String, UserDetail> owners = new HashMap<>();
        for (String connectionId : connectionIds) {
            DataSourceConnectionDto connection = byId.get(connectionId);
            if (connection == null || !AccessNodeTypeEnum.isManually(connection.getAccessNodeType())
                    || StringUtils.isBlank(connection.getAccessNodeProcessId())) {
                continue;
            }
            UserDetail connectionUser = AccessNodeTypeEnum.isGroupManually(connection.getAccessNodeType())
                    ? policyOwner(connection.getUserId(), user, owners) : user;
            Set<String> connectionAgents = connectionAllowedAgents(connection, connectionUser);
            if (allowedAgentIntersection == null) {
                allowedAgentIntersection = new LinkedHashSet<>(connectionAgents);
            } else {
                allowedAgentIntersection.retainAll(connectionAgents);
            }
            restrictedConnectionNames.add(sourceConnectionName(connection));
            if (StringUtils.isNotBlank(connection.getPriorityProcessId())) {
                priorityAgents.add(connection.getPriorityProcessId());
            }
        }
        return new ConnectionAgentPolicy(allowedAgentIntersection, restrictedConnectionNames, priorityAgents);
    }

    private UserDetail policyOwner(String ownerId, UserDetail actingUser, Map<String, UserDetail> owners) {
        if (settingsService == null || !settingsService.isCloud() || StringUtils.isBlank(ownerId)
                || (actingUser != null && ownerId.equals(actingUser.getUserId()))) {
            return actingUser;
        }
        return owners.computeIfAbsent(ownerId, id -> {
            UserDetail owner = userService.loadUserById(new ObjectId(id));
            Assert.notNull(owner, "Agent policy owner not found");
            return owner;
        });
    }

    private boolean hasTaskAgentPolicyOverride(TaskDto taskDto) {
        return AccessNodeTypeEnum.isManually(taskDto.getAccessNodeType())
                && StringUtils.isNotBlank(taskDto.getAccessNodeProcessId());
    }

    private boolean isUnresolvableGroup(BizException exception) {
        return AgentGroupTag.GROUP_NOT_FUND.equals(exception.getErrorCode())
                || "group.agent.not.available".equals(exception.getErrorCode())
                || "group.agent.not.invalid".equals(exception.getErrorCode());
    }

    private Set<String> connectionAllowedAgents(DataSourceConnectionDto connection, UserDetail user) {
        if (AccessNodeTypeEnum.isGroupManually(connection.getAccessNodeType())) {
            try {
                List<String> processIds = agentGroupService.getProcessNodeListWithGroup(connection, user);
                return new LinkedHashSet<>(processIds == null ? Collections.emptyList() : processIds);
            } catch (BizException e) {
                if (!isUnresolvableGroup(e)) {
                    throw e;
                }
                return Collections.emptySet();
            }
        }
        return new LinkedHashSet<>(Collections.singletonList(connection.getAccessNodeProcessId()));
    }

    private Set<String> taskAllowedAgents(TaskDto taskDto, UserDetail user) {
        if (AccessNodeTypeEnum.isGroupManually(taskDto.getAccessNodeType())) {
            try {
                List<String> processIds = agentGroupService.getProcessNodeListWithGroup(taskDto,
                        policyOwner(taskDto.getUserId(), user, new HashMap<>()));
                return new LinkedHashSet<>(processIds == null ? Collections.emptyList() : processIds);
            } catch (BizException e) {
                if (!isUnresolvableGroup(e)) {
                    throw e;
                }
                return Collections.emptySet();
            }
        }
        return new LinkedHashSet<>(Collections.singletonList(taskDto.getAccessNodeProcessId()));
    }

    private TaskDto runtimeTaskForAgentSet(TaskDto taskDto, Set<String> allowedAgents,
                                           List<String> connectionPriorities, UserDetail user) {
        List<String> candidates = new ArrayList<>(allowedAgents);
        List<Worker> availableAgents = workerService.findAvailableAgentByAccessNode(user, candidates);
        String selectedAgent = selectRuntimeAgent(taskDto, candidates, connectionPriorities, availableAgents);
        if (StringUtils.isBlank(selectedAgent)) {
            return null;
        }

        TaskDto schedulingTaskDto = new TaskDto();
        BeanUtils.copyProperties(taskDto, schedulingTaskDto);
        schedulingTaskDto.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        schedulingTaskDto.setAccessNodeProcessId(selectedAgent);
        schedulingTaskDto.setPriorityProcessId(selectedAgent);
        schedulingTaskDto.setAgentId(null);
        return schedulingTaskDto;
    }

    private String selectRuntimeAgent(TaskDto taskDto, List<String> candidates,
                                      List<String> connectionPriorities, List<Worker> availableAgents) {
        if (CollectionUtils.isEmpty(availableAgents)) {
            return null;
        }
        Set<String> availableIds = availableAgents.stream()
                .map(Worker::getProcessId)
                .filter(StringUtils::isNotBlank)
                .filter(candidates::contains)
                .collect(Collectors.toCollection(TreeSet::new));
        if (StringUtils.isNotBlank(taskDto.getPriorityProcessId()) && availableIds.contains(taskDto.getPriorityProcessId())) {
            return taskDto.getPriorityProcessId();
        }
        for (String priority : connectionPriorities) {
            if (availableIds.contains(priority)) {
                return priority;
            }
        }
        return availableIds.stream().findFirst().orElse(null);
    }

    private List<String> taskConnectionIds(TaskDto taskDto) {
        if (taskDto.getDag() == null) {
            return Collections.emptyList();
        }
        if (TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(taskDto.getSyncType())) {
            if (CollectionUtils.isEmpty(taskDto.getDag().getSources())) {
                return Collections.emptyList();
            }
            return taskDto.getDag().getSources().stream()
                    .filter(LogCollectorNode.class::isInstance)
                    .map(LogCollectorNode.class::cast)
                    .map(LogCollectorNode::getConnectionIds)
                    .filter(CollectionUtils::isNotEmpty)
                    .flatMap(connectionIds -> connectionIds.stream())
                    .filter(StringUtils::isNotBlank)
                    .distinct()
                    .collect(Collectors.toList());
        }
        if (TaskDto.SYNC_TYPE_CONN_HEARTBEAT.equals(taskDto.getSyncType())) {
            if (CollectionUtils.isEmpty(taskDto.getDag().getTargets())) {
                return Collections.emptyList();
            }
            return taskDto.getDag().getTargets().stream()
                    .filter(DataParentNode.class::isInstance)
                    .map(node -> (DataParentNode<?>) node)
                    .map(DataParentNode::getConnectionId)
                    .filter(StringUtils::isNotBlank)
                    .distinct()
                    .collect(Collectors.toList());
        }
        if (CollectionUtils.isEmpty(taskDto.getDag().getNodes())) {
            return Collections.emptyList();
        }
        return taskDto.getDag().getNodes().stream()
                .filter(DataParentNode.class::isInstance)
                .map(node -> (DataParentNode<?>) node)
                .map(DataParentNode::getConnectionId)
                .filter(StringUtils::isNotBlank)
                .distinct()
                .collect(Collectors.toList());
    }

    private String sourceConnectionName(DataSourceConnectionDto connection) {
        if (StringUtils.isNotBlank(connection.getName())) {
            return connection.getName();
        }
        return connection.getId() == null ? "unknown" : connection.getId().toHexString();
    }

    public void handleScheduleLimit(WorkerDto workerDto, UserDetail user) {
        UserDetail finalUser = user;
        List<Worker> workerList = workerService.findAvailableAgent(finalUser);
        if (workerList.size() == 1) {
            if (workerList.get(0).getProcessId().equals(workerDto.getProcessId())) {
                throw new BizException("Task.ScheduleLimit");
            } else {
                throw new BizException("Task.ManuallyScheduleLimit", workerList.get(0).getProcessId());
            }
        }
        List<String> availableAgent = new ArrayList();
        workerList.stream().forEach(worker -> {
            if (!worker.getProcessId().equals(workerDto.getProcessId())) {
                int runningTaskNum = taskService.runningTaskNum(worker.getProcessId(), finalUser);
                WorkerDto workerDtoTemp = new WorkerDto();
                BeanUtil.copyProperties(worker, workerDtoTemp);
                int limitTaskNumTemp = workerService.getLimitTaskNum(workerDtoTemp, finalUser);
                if (runningTaskNum < limitTaskNumTemp) {
                    availableAgent.add(worker.getTcmInfo().getAgentName());
                }
            }
        });
        throw new BizException("Task.ManuallyScheduleLimit", JSON.toJSONString(availableAgent));
    }

}
