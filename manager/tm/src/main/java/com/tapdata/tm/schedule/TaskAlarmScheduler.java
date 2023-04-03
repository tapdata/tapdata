package com.tapdata.tm.schedule;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import io.tapdata.common.executor.ExecutorsManager;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskAlarmScheduler {

    private TaskService taskService;
    private AlarmService alarmService;
    private WorkerService workerService;
    private UserService userService;
    private SettingsService settingsService;

    private MeasurementServiceV2 measurementServiceV2;

    private final ExecutorService executorService = ExecutorsManager.getInstance().getExecutorService();

    @Scheduled(cron = "0 0/5 * * * ? ")
    @SchedulerLock(name ="task_agent_alarm_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void taskAgentAlarm() {
        Object buildProfile = settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.BUILD_PROFILE);
        if (Objects.isNull(buildProfile)) {
            buildProfile = "DAAS";
        }
        boolean isCloud = buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");

        Query query = new Query(Criteria.where("status").is(TaskDto.STATUS_RUNNING)
                .and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                .and("is_deleted").is(false));
        List<TaskDto> taskDtos = taskService.findAll(query);
        if (CollectionUtils.isEmpty(taskDtos)) {
            return;
        }

        List<String> collect = taskDtos.stream().map(TaskDto::getAgentId).distinct().collect(Collectors.toList());

        Query worerQuery = new Query(Criteria.where("worker_type").is("connector")
                .and("process_id").in(collect));
        List<WorkerDto> workers = workerService.findAll(worerQuery);
        if (CollectionUtils.isEmpty(workers)) {
            return;
        }

        Object heartTime = settingsService.getValueByCategoryAndKey(CategoryEnum.WORKER, KeyEnum.WORKER_HEART_TIMEOUT);
        long heartExpire = Objects.nonNull(heartTime) ? (Long.parseLong(heartTime.toString()) + 48 ) * 1000 : 108000;

        Map<String, WorkerDto> stopEgineMap = workers.stream().filter(w -> (Objects.nonNull(w.getIsDeleted()) && w.getIsDeleted()) ||
                        (Objects.nonNull(w.getStopping()) && w.getStopping()) ||
                        w.getPingTime() < (System.currentTimeMillis() - heartExpire))
                .collect(Collectors.toMap(WorkerDto::getProcessId, Function.identity(), (e1, e2) -> e1));
        if (stopEgineMap.isEmpty()) {
            return;
        } else {
            log.error("taskAgentAlarm stopEgineMap data:{}", JSON.toJSONString(stopEgineMap));
        }

        Set<String> agentIds = stopEgineMap.keySet();

        // 云版需要修改这里
        List<TaskDto> taskList = taskDtos.stream().filter(t -> agentIds.contains(t.getAgentId())).collect(Collectors.toList());

        List<String> userIds = taskList.stream().map(TaskDto::getUserId).distinct().collect(Collectors.toList());
        List<UserDetail> userByIdList = userService.getUserByIdList(userIds);
        Map<String, UserDetail> userDetailMap = userByIdList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity(), (e1, e2) -> e1));

        for (TaskDto data : taskList) {
            UserDetail userDetail = userDetailMap.get(data.getUserId());
            boolean checkOpen = alarmService.checkOpen(data, null, AlarmKeyEnum.SYSTEM_FLOW_EGINGE_DOWN, null, userDetail);
            if (!checkOpen) {
                continue;
            }

            List<Worker> workerList = workerService.findAvailableAgentBySystem(userDetail);;
            if (AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.getName().equals(data.getAccessNodeType())) {
                List<String> processIdList = data.getAccessNodeProcessIdList();
                workerList = workerList.stream().filter(w -> processIdList.contains(w.getProcessId())).collect(Collectors.toList());
            }

            String orginAgentId = data.getAgentId();
            AtomicReference<String> summary = new AtomicReference<>();
            Map<String, Object> param = Maps.newHashMap();
            String alarmDate = DateUtil.now();
            param.put("alarmDate", alarmDate);
            param.put("taskName", data.getName());
            param.put("agentName", orginAgentId);
            if (CollectionUtils.isEmpty(workerList)) {
                FunctionUtils.isTureOrFalse(isCloud).trueOrFalseHandle(
                        () -> summary.set("SYSTEM_FLOW_EGINGE_DOWN_CLOUD"),
                        () -> summary.set("SYSTEM_FLOW_EGINGE_DOWN_NO_AGENT")
                );
            } else {
                if (isCloud) {
                    summary.set("SYSTEM_FLOW_EGINGE_DOWN_CLOUD");
                } else {
                    data.setAgentId(null);
                    CalculationEngineVo calculationEngineVo = workerService.scheduleTaskToEngine(data, userDetail, "task", data.getName());
                    param.put("number", workerList.size());
                    param.put("otherAgentName", calculationEngineVo.getProcessId());
                    summary.set("SYSTEM_FLOW_EGINGE_DOWN_CHANGE_AGENT");
                    orginAgentId = calculationEngineVo.getProcessId();
                }

                if (!isCloud) {
                    taskService.start(data, userDetail, "11");
                }
            }

            AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.WARNING).component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(orginAgentId).taskId(data.getId().toHexString())
                    .name(data.getName()).summary(summary.get()).metric(AlarmKeyEnum.SYSTEM_FLOW_EGINGE_DOWN)
                    .build();
            alarmInfo.setParam(param);
            alarmInfo.setUserId(data.getUserId());
            alarmService.save(alarmInfo);
        }
    }
}
