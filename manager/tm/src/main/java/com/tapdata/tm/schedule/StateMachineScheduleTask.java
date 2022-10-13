/**
 * @title: StateMachineScheduleTask
 * @description:
 * @author lk
 * @date 2021/11/29
 */
package com.tapdata.tm.schedule;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.TaskState;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;
import static org.springframework.data.mongodb.core.query.Query.query;

@Slf4j
@Component
@Setter(onMethod_ = {@Autowired})
public class StateMachineScheduleTask {
	private StateMachineService stateMachineService;
	private UserService userService;
	private WorkerService workerService;
	private SettingsService settingsService;
	private TaskService taskService;

	//@Scheduled(fixedDelay = 5 * 1000)
	//@SchedulerLock(name ="checkScheduledTask", lockAtMostFor = "5s", lockAtLeastFor = "5s")
	public void checkScheduledTask() {
		Query query = query(Criteria.where("status").is(TaskState.WAITING_RUN.getName()).and("scheduledTime").lt(new Date(System.currentTimeMillis() - 1000 * 60)));
		List<TaskDto> taskDtos = taskService.findAll(query);
		taskDtos.forEach(taskDto -> {
			try {
				UserDetail userDetail = userService.loadUserById(toObjectId(taskDto.getUserId()));
				StateMachineResult result = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, userDetail);
				log.info("checkScheduledTask complete, result: {}", JsonUtil.toJson(result));
			} catch (Throwable e) {
				log.error("Failed to execute state machine,taskId: {}, event: {},message: {}", taskDto.getId().toHexString(), DataFlowEvent.OVERTIME.getName(), e.getMessage(), e);
			}
		});
	}

	//@Scheduled(fixedDelay = 5 * 1000)
	//@SchedulerLock(name ="checkStoppingTask", lockAtMostFor = "5s", lockAtLeastFor = "5s")
	public void checkStoppingTask() {
		Query query = query(Criteria.where("status").is(TaskState.STOPPING.getName()).and("stoppingTime").lt(new Date(System.currentTimeMillis() - 1000 * 60 * 5)));
		List<TaskDto> taskDtos = taskService.findAll(query);
		taskDtos.forEach(taskDto -> {
			try {
				UserDetail userDetail = userService.loadUserById(toObjectId(taskDto.getUserId()));
				StateMachineResult result = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, userDetail);
				log.info("checkStoppingTask complete, result: {}", JsonUtil.toJson(result));
			} catch (Throwable e) {
				log.error("Failed to execute state machine,taskId: {}, event: {},message: {}", taskDto.getId().toHexString(), DataFlowEvent.OVERTIME.getName(), e.getMessage(), e);
			}
		});
	}

	@Scheduled(fixedDelay = 5 * 1000)
	@SchedulerLock(name ="checkScheduledDataFlow", lockAtMostFor = "5s", lockAtLeastFor = "5s")
	public void checkScheduledDataFlow() {

		//云版不需要这个重新调度的逻辑
		Object buildProfile = settingsService.getByCategoryAndKey("System", "buildProfile");
		final boolean isCloud = true;//buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");


		Object jobHeartTimeout = settingsService.getValueByCategoryAndKey(CategoryEnum.JOB, KeyEnum.JOB_HEART_TIMEOUT);
		if (jobHeartTimeout == null || Long.parseLong(jobHeartTimeout.toString()) <= 0){
			log.warn("The setting of jobHeartTimeout must be greater than 0, jobHeartTimeout: {}", jobHeartTimeout);
			return;
		}
		int timeoutMillis = Integer.parseInt(jobHeartTimeout.toString());

		long checkTime = System.currentTimeMillis() - timeoutMillis;

		DateTime dateTime = DateUtil.date(checkTime);

		Query query = Query.query(new Criteria("status").in(TaskDto.STATUS_SCHEDULING)
				.orOperator(Criteria.where("pingTime").lt(dateTime), Criteria.where("pingTime").lt(checkTime)));
		List<TaskDto> taskDtos = taskService.findAll(query);
		taskDtos.forEach(taskDto -> {
			try {
				//如果任务的agent掉线了，在云版环境是不能重新调度的，云版的agent与tm的网络环境不会很好，掉线是经常的事情。
				//但是如果agent如果已经被删除了，是应该重新调度的。
				UserDetail userDetail = userService.loadUserById(toObjectId(taskDto.getUserId()));
				if (isCloud) {
					String status = workerService.checkUsedAgent(taskDto.getAgentId(), userDetail);
					if ("offline".equals(status)) {
						log.debug("The cloud version does not need this rescheduling");
						return;
					}
				}

				log.info("checkScheduledDataFlow start,dataFlowId: {}, status: {}", taskDto.getId().toHexString(), taskDto.getStatus());
				if (taskDto.getRestartFlag()){
					taskDto.setAgentId(null);
					workerService.scheduleTaskToEngine(taskDto, userDetail, "task", taskDto.getName());
					String processId = taskDto.getAgentId();
					log.info("checkScheduledDataFlow complete,dataFlowId: {}, processId: {}", taskDto.getId().toHexString(), processId);

					taskService.run(taskDto, userDetail);
				} else {
					taskDto.setRestartFlag(true);
					taskService.save(taskDto, userDetail);
				}

			} catch (Throwable e) {
				log.error("checkScheduledDataFlow Failed to execute state machine,dataFlowId: {}, event: {},message: {}", taskDto.getId().toHexString(), DataFlowEvent.OVERTIME.getName(), e.getMessage(), e);
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

		List<TaskDto> taskDtos = taskService.findAll(query);
		Map<String, UserDetail> userDetailMap = new HashMap<>();
		for (TaskDto taskDto : taskDtos) {
			UserDetail userDetail = userDetailMap.get(taskDto.getUserId());
			if (userDetail == null) {
				userDetail = userService.loadUserById(toObjectId(taskDto.getUserId()));
				userDetailMap.put(taskDto.getUserId(), userDetail);
			}

			taskService.run(taskDto, userDetail);
		}



	}
}
