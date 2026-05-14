package com.tapdata.tm.cluster.service;

import com.tapdata.tm.cluster.dto.ComponentStoppedRequest;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TransformSchemaService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.entity.field.WorkerType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@Setter(onMethod_ = {@Autowired})
public class ClusterComponentStopService {

    private MongoTemplate mongoTemplate;
    private TaskService taskService;
    private TaskScheduleService taskScheduleService;
    private StateMachineService stateMachineService;
    private TransformSchemaService transformSchema;
    private UserService userService;

    public Map<String, Object> componentStopped(ComponentStoppedRequest req, UserDetail caller) {
        log.info("ClusterComponent event=received component={} uuid={} processId={} callerUserId={}",
                req == null ? null : req.getComponent(),
                req == null ? null : req.getUuid(),
                req == null ? null : req.getProcessId(),
                caller == null ? null : caller.getUserId());
        validate(req);

        String uuid = req.getUuid();
        String component = req.getComponent();
        String processId = req.getProcessId();

        Map<String, Object> result = new HashMap<>();
        int taskRescheduled = 0;
        boolean workerUpdated = false;
        boolean clusterStateUpdated;

        switch (component) {
            case ComponentStoppedRequest.COMPONENT_ENGINE:
                workerUpdated = markWorkerStopped(processId, "connector");
                taskRescheduled = failoverTasksOf(processId);
                clusterStateUpdated = setClusterStateComponentStopped(uuid, "engine");
                break;
            case ComponentStoppedRequest.COMPONENT_APISERVER:
                workerUpdated = markWorkerStopped(processId, WorkerType.API_SERVER.getType());
                clusterStateUpdated = setClusterStateComponentStopped(uuid, "apiServer");
                break;
            case ComponentStoppedRequest.COMPONENT_FRONTEND:
                clusterStateUpdated = setClusterStateComponentStopped(uuid, "management");
                break;
            default:
                throw new IllegalArgumentException("Unknown component: " + component);
        }

        log.info("ClusterComponent event=stopped component={} uuid={} processId={} workerUpdated={} clusterStateUpdated={} taskRescheduled={} reason=agent_initiated",
                component, uuid, processId, workerUpdated, clusterStateUpdated, taskRescheduled);

        result.put("workerUpdated", workerUpdated);
        result.put("clusterStateUpdated", clusterStateUpdated);
        result.put("taskRescheduled", taskRescheduled);
        return result;
    }

    private void validate(ComponentStoppedRequest req) {
        if (req == null) throw new IllegalArgumentException("request is null");
        if (StringUtils.isBlank(req.getUuid())) throw new IllegalArgumentException("uuid is required");
        if (StringUtils.isBlank(req.getComponent())) throw new IllegalArgumentException("component is required");
        String c = req.getComponent();
        boolean engineOrApi = ComponentStoppedRequest.COMPONENT_ENGINE.equals(c)
                || ComponentStoppedRequest.COMPONENT_APISERVER.equals(c);
        if (engineOrApi && StringUtils.isBlank(req.getProcessId())) {
            throw new IllegalArgumentException("processId is required for component=" + c);
        }
    }

    private boolean markWorkerStopped(String processId, String workerType) {
        Query query = Query.query(Criteria.where("process_id").is(processId)
                .and("worker_type").is(workerType));
        Update update = new Update()
                .set("stopping", true)
                .set("ping_time", 0L);
        long modified = mongoTemplate.updateMulti(query, update, "Workers").getModifiedCount();
        if (modified == 0) {
            log.warn("ClusterComponent markWorkerStopped: no Worker found processId={} workerType={}", processId, workerType);
        }
        return modified > 0;
    }

    private boolean setClusterStateComponentStopped(String uuid, String componentField) {
        Query query = Query.query(Criteria.where("systemInfo.uuid").is(uuid));
        Update update = new Update()
                .set(componentField + ".status", "stopped")
                .set("ttl", new Date(System.currentTimeMillis() - 1000L))
                .set("last_updated", new Date());
        long modified = mongoTemplate.updateMulti(query, update, "ClusterState").getModifiedCount();
        if (modified == 0) {
            log.warn("ClusterComponent setClusterStateComponentStopped: no ClusterState found uuid={} field={}", uuid, componentField);
        }
        return modified > 0;
    }

    private int failoverTasksOf(String processId) {
        Criteria criteria = Criteria.where("agentId").is(processId)
                .and("status").in(TaskDto.STATUS_RUNNING, TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN);
        List<TaskDto> tasks = taskService.findAll(Query.query(criteria));
        if (CollectionUtils.isEmpty(tasks)) {
            log.info("TaskHA event=agent_initiated_offline processId={} taskCount=0 reason=no_running_tasks", processId);
            return 0;
        }

        List<String> userIds = tasks.stream().map(TaskDto::getUserId).distinct().collect(Collectors.toList());
        Map<String, UserDetail> userMap = userService.getUserMapByIdList(userIds);

        int rescheduled = 0;
        for (TaskDto taskDto : tasks) {
            UserDetail user = userMap.get(taskDto.getUserId());
            if (user == null) {
                log.warn("TaskHA event=agent_initiated_offline skip taskId={} reason=user_not_found userId={}",
                        taskDto.getId().toHexString(), taskDto.getUserId());
                continue;
            }
            try {
                StateMachineResult sm = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, user);
                if (sm == null || sm.isFail()) {
                    log.warn("TaskHA event=agent_initiated_offline skip taskId={} reason=state_machine_reject", taskDto.getId().toHexString());
                    continue;
                }
                transformSchema.transformSchemaBeforeDynamicTableName(taskDto, user);
                taskScheduleService.scheduling(taskDto, user, true);
                rescheduled++;
            } catch (Exception e) {
                log.warn("TaskHA event=agent_initiated_offline skip taskId={} reason=exception",
                        taskDto.getId().toHexString(), e);
            }
        }
        log.info("TaskHA event=agent_initiated_offline processId={} taskCount={} rescheduled={}",
                processId, tasks.size(), rescheduled);
        return rescheduled;
    }

}
