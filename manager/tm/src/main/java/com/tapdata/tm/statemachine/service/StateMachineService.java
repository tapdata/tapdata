/**
 * @title: DataFlowStateService
 * @description:
 * @author lk
 * @date 2021/11/15
 */
package com.tapdata.tm.statemachine.service;

import cn.hutool.core.date.StopWatch;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.StateMachine;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.DataFlowState;
import com.tapdata.tm.statemachine.enums.TaskState;
import com.tapdata.tm.statemachine.model.DataFlowStateTrigger;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.TaskStateTrigger;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.FunctionUtils;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

@Service
@Slf4j
public class StateMachineService {

	@Autowired
	StateMachine<DataFlowState, DataFlowEvent> dataFlowMachine;
	@Autowired
	StateMachine<TaskState, DataFlowEvent> taskMachine;

	@Autowired
	DataFlowService dataFlowService;

	@Autowired
	private TaskService taskService;

	@Autowired
	private UserService userService;
	@Autowired
	private MonitoringLogsService monitoringLogsService;

	public StateMachineResult test(String id, String event, String status){
		DataFlowState state = DataFlowState.getState(status);
		DataFlowStateTrigger dataFlowStateTrigger = new DataFlowStateTrigger();
		dataFlowStateTrigger.setSource(state);
		dataFlowStateTrigger.setEvent(DataFlowEvent.getEvent(event));
		DataFlowDto dto = dataFlowService.findOne(new Query());
		dataFlowStateTrigger.setData(dto);
		return dataFlowMachine.handleEvent(dataFlowStateTrigger);
	}

	public StateMachineResult testTask(String id, String event, String status){
		return executeAboutTask(id, event, userService.loadUserById(toObjectId("61408608c4e5c40012663090")));
	}

	public StateMachineResult executeAboutDataFlow(String dataFlowId, String dataFlowEvent, UserDetail userDetail){
		DataFlowEvent event = DataFlowEvent.getEvent(dataFlowEvent);
		if (event == null){
			throw new BizException(String.format("DataFlow event[%s] does not exist", dataFlowEvent));
		}
		return executeAboutDataFlow(toObjectId(dataFlowId), event , userDetail);
	}

	public StateMachineResult executeAboutDataFlow(ObjectId dataFlowId, DataFlowEvent event, UserDetail userDetail){
		DataFlowDto dto = dataFlowService.findById(dataFlowId, userDetail);
		if (dto == null){
			throw new BizException("DataFlow.Not.Found");
		}
		return executeAboutDataFlow(dto, event, userDetail);

	}

	public StateMachineResult executeAboutDataFlow(DataFlowDto dto, DataFlowEvent event, UserDetail userDetail){
		DataFlowStateTrigger trigger = new DataFlowStateTrigger();
		trigger.setSource(DataFlowState.getState(dto.getStatus()));
		trigger.setEvent(event);
		trigger.setUserDetail(userDetail);
		trigger.setData(dto);
		return executeAboutDataFlow(trigger);
	}

	private StateMachineResult executeAboutDataFlow(DataFlowStateTrigger trigger){
		return dataFlowMachine.handleEvent(trigger);
	}

	public StateMachineResult executeAboutTask(String taskId, String taskEvent, UserDetail userDetail){
		DataFlowEvent event = DataFlowEvent.getEvent(taskEvent);
		if (event == null){
			throw new BizException(String.format("task event[%s] does not exist", taskEvent));
		}
		return executeAboutTask(toObjectId(taskId), event , userDetail);

	}

	public StateMachineResult executeAboutTask(ObjectId taskId, DataFlowEvent event, UserDetail userDetail){
		TaskDto dto = taskService.findById(taskId, userDetail);
		if (dto == null){
			throw new BizException("Task.NotFound");
		}
		return executeAboutTask(dto, event, userDetail);

	}

	public StateMachineResult executeAboutTask(TaskDto dto, DataFlowEvent event, UserDetail userDetail) {
		TaskStateTrigger trigger = new TaskStateTrigger();
		trigger.setSource(TaskState.getState(dto.getStatus()));
		trigger.setEvent(event);
		trigger.setUserDetail(userDetail);
		trigger.setData(dto);

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		StateMachineResult stateMachineResult = executeAboutTask(trigger);
		stopWatch.stop();

		CompletableFuture.runAsync(() -> {
			monitoringLogsService.taskStateMachineLog(dto, userDetail, event, stateMachineResult, stopWatch.getTotalTimeMillis());

			FunctionUtils.isTureOrFalse(stateMachineResult.isOk()).trueOrFalseHandle(
					() -> taskService.updateTaskRecordStatus(dto, stateMachineResult.getAfter(), userDetail),
					() -> {
						monitoringLogsService.startTaskErrorLog(dto, userDetail, new BizException("concurrent start operations, this operation don‘t effective"));
						taskService.updateTaskRecordStatus(dto, stateMachineResult.getBefore(), userDetail);
					}
			);
		});

		return stateMachineResult;
	}

	private StateMachineResult executeAboutTask(TaskStateTrigger trigger){
		return taskMachine.handleEvent(trigger);
	}
}
