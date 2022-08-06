/**
 * @title: StateMachineScheduleTask
 * @description:
 * @author lk
 * @date 2021/11/29
 */
package com.tapdata.tm.schedule;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.CustomerJobLogs.CustomerJobLog;
import com.tapdata.tm.CustomerJobLogs.service.CustomerJobLogsService;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.DataFlowState;
import com.tapdata.tm.statemachine.enums.TaskState;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;

import java.util.*;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import static org.springframework.data.mongodb.core.query.Query.query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Setter(onMethod_ = {@Autowired})
public class StateMachineScheduleTask {
	private StateMachineService stateMachineService;
	private UserService userService;
	private DataFlowService dataFlowService;
	private WorkerService workerService;
	private CustomerJobLogsService customerJobLogsService;
	private SettingsService settingsService;
	private TaskService taskService;

	@Scheduled(fixedDelay = 5 * 1000)
	@SchedulerLock(name ="checkScheduledTask", lockAtMostFor = "5s", lockAtLeastFor = "5s")
	public void checkScheduledTask() {
		Query query = query(Criteria.where("status").is(TaskState.WAITING_RUN.getName()).and("scheduledTime").lt(new Date(System.currentTimeMillis() - 1000 * 60)));
		List<TaskDto> taskDtos = taskService.findAll(query);
		taskDtos.forEach(taskDto -> {
			try {
				UserDetail userDetail = userService.loadUserById(toObjectId(taskDto.getUserId()));
				StateMachineResult result = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, userDetail);
				log.info("checkScheduledSubTask complete, result: {}", JsonUtil.toJson(result));
			} catch (Throwable e) {
				log.error("Failed to execute state machine,subTaskId: {}, event: {},message: {}", taskDto.getId().toHexString(), DataFlowEvent.OVERTIME.getName(), e.getMessage(), e);
			}
		});
	}

	@Scheduled(fixedDelay = 5 * 1000)
	@SchedulerLock(name ="checkStoppingTask", lockAtMostFor = "5s", lockAtLeastFor = "5s")
	public void checkStoppingSubTask() {
		Query query = query(Criteria.where("status").is(TaskState.STOPPING.getName()).and("stoppingTime").lt(new Date(System.currentTimeMillis() - 1000 * 60 * 5)));
		List<TaskDto> taskDtos = taskService.findAll(query);
		taskDtos.forEach(taskDto -> {
			try {
				UserDetail userDetail = userService.loadUserById(toObjectId(taskDto.getUserId()));
				StateMachineResult result = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, userDetail);
				log.info("checkStoppingSubTask complete, result: {}", JsonUtil.toJson(result));
			} catch (Throwable e) {
				log.error("Failed to execute state machine,subTaskId: {}, event: {},message: {}", taskDto.getId().toHexString(), DataFlowEvent.OVERTIME.getName(), e.getMessage(), e);
			}
		});
	}

	/**
	 * dataflow心跳超时处理
	 * 企业版:  调度中会重新分配agentId,一直调度中
	 *         运行中会重新分配agentId, 分配后变为调度中
	 *         停止中（强制停止中）超过5倍心跳超时时间会变为已暂停
	 * 云版:   调度中不会分配agentId,一直调度中
	 *         运行中会变为调度中,不会分配agentId,
	 *         停止中（强制停止中）超过5倍心跳超时时间会变为已暂停
	 **/
	@Scheduled(fixedDelay = 5 * 1000)
	@SchedulerLock(name ="checkScheduledDataFlow", lockAtMostFor = "5s", lockAtLeastFor = "5s")
	public void checkScheduledDataFlow() {
		Object jobHeartTimeout = settingsService.getValueByCategoryAndKey(CategoryEnum.JOB, KeyEnum.JOB_HEART_TIMEOUT);
		if (jobHeartTimeout == null || Long.parseLong(jobHeartTimeout.toString()) <= 0){
			log.warn("The setting of jobHeartTimeout must be greater than 0, jobHeartTimeout: {}", jobHeartTimeout);
			return;
		}
		long timeoutMillis = Long.parseLong(jobHeartTimeout.toString());
		Object buildProfile = settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.BUILD_PROFILE);
		boolean isCloud = "CLOUD".equals(buildProfile) || "DRS".equals(buildProfile) || "DFS".equals(buildProfile);
		List<String> statusList = new ArrayList<>();
		statusList.add(DataFlowState.RUNNING.getName());
		//  任务心跳超时在云版情况下不会重新设置agentId，所以scheduling状态下的任务不做处理，直到它被接管running为止
		if (!isCloud){
			statusList.add(DataFlowState.SCHEDULING.getName());
		}

		checkScheduledTask(timeoutMillis, isCloud);

		Query query = Query.query(new Criteria().orOperator(
				new Criteria("status").in(statusList).and("pingTime").lt(System.currentTimeMillis() - timeoutMillis),
				new Criteria("status").in(DataFlowState.STOPPING.getName(), DataFlowState.FORCE_STOPPING.getName())
						.and("pingTime").lt(System.currentTimeMillis() - timeoutMillis * 5)
		));
		List<DataFlowDto> dataFlowDtos = dataFlowService.findAll(query);
		dataFlowDtos.forEach(dataFlowDto -> {
			try {
				log.info("checkScheduledSubTask start,dataFlowId: {}, status: {}", dataFlowDto.getId().toHexString(), dataFlowDto.getStatus());
				UserDetail userDetail = userService.loadUserById(toObjectId(dataFlowDto.getUserId()));
				if (DataFlowState.SCHEDULING.getName().equals(dataFlowDto.getStatus())){
					dataFlowDto.setAgentId(null);
					workerService.scheduleTaskToEngine(dataFlowDto, userDetail);
					String processId = dataFlowDto.getAgentId();
					if (StringUtils.isNotBlank(processId)){
						UpdateResult result = dataFlowService.update(
								query(Criteria.where("_id").is(dataFlowDto.getId()).and("status").is(DataFlowState.SCHEDULING.getName())),
								Update.update("agentId", processId).set("pingTime", System.currentTimeMillis()), userDetail);
						if (result.wasAcknowledged() && result.getModifiedCount() > 0){
							CustomerJobLog dataFlowLog = new CustomerJobLog(dataFlowDto.getId().toString(),dataFlowDto.getName(), CustomerJobLogsService.DataFlowType.clone);
							WorkerDto workerDto = workerService.findOne(new Query(Criteria.where("process_id").is(processId)));
							dataFlowLog.setAgentHost(workerDto.getHostname());
							customerJobLogsService.assignAgent(dataFlowLog, userDetail);
						}
					}
					log.info("checkScheduledSubTask complete,dataFlowId: {}, processId: {}", dataFlowDto.getId().toHexString(), processId);
				}else {
					StateMachineResult result = stateMachineService.executeAboutDataFlow(dataFlowDto, DataFlowEvent.OVERTIME, userDetail);
					log.info("checkScheduledSubTask complete,dataFlowId: {}, result: {}", dataFlowDto.getId().toHexString(), JsonUtil.toJson(result));
				}

			} catch (Throwable e) {
				log.error("Failed to execute state machine,dataFlowId: {}, event: {},message: {}", dataFlowDto.getId().toHexString(), DataFlowEvent.OVERTIME.getName(), e.getMessage(), e);
			}
		});
	}


	public void checkScheduledTask(long timeoutMillis, boolean isCloud) {
		List<String> statusList = new ArrayList<>();
		statusList.add(TaskDto.STATUS_RUNNING);
		//  任务心跳超时在云版情况下不会重新设置agentId，所以scheduling状态下的任务不做处理，直到它被接管running为止
		if (!isCloud){
			statusList.add(TaskDto.STATUS_SCHEDULING);
		}

		Query query = Query.query(new Criteria().orOperator(
				new Criteria("status").in(statusList).and("pingTime").lt(System.currentTimeMillis() - timeoutMillis),
				new Criteria("status").is(TaskDto.STATUS_STOPPING)
						.and("pingTime").lt(System.currentTimeMillis() - timeoutMillis * 5)
		));

		List<TaskDto> subTaskDtos = taskService.findAll(query);
		Map<String, UserDetail> userDetailMap = new HashMap<>();
		for (TaskDto taskDto : subTaskDtos) {
			UserDetail userDetail = userDetailMap.get(taskDto.getUserId());
			if (userDetail == null) {
				userDetail = userService.loadUserById(toObjectId(taskDto.getUserId()));
				userDetailMap.put(taskDto.getUserId(), userDetail);
			}

			taskService.run(taskDto, userDetail);
		}



	}
}
