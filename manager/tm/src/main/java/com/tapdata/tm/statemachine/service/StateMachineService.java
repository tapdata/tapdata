/**
 * @title: DataFlowStateService
 * @description:
 * @author lk
 * @date 2021/11/15
 */
package com.tapdata.tm.statemachine.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.statemachine.StateMachine;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.DataFlowState;
import com.tapdata.tm.statemachine.enums.SubTaskState;
import com.tapdata.tm.statemachine.model.DataFlowStateTrigger;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.SubTaskStateTrigger;
import com.tapdata.tm.task.service.SubTaskService;
import com.tapdata.tm.user.service.UserService;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class StateMachineService {

	@Autowired
	StateMachine<DataFlowState, DataFlowEvent> dataFlowMachine;
	@Autowired
	StateMachine<SubTaskState, DataFlowEvent> subTaskMachine;

	@Autowired
	DataFlowService dataFlowService;

	@Autowired
	private SubTaskService subTaskService;

	@Autowired
	private UserService userService;

	public StateMachineResult test(String id, String event, String status){
		DataFlowState state = DataFlowState.getState(status);
		DataFlowStateTrigger dataFlowStateTrigger = new DataFlowStateTrigger();
		dataFlowStateTrigger.setSource(state);
		dataFlowStateTrigger.setEvent(DataFlowEvent.getEvent(event));
		DataFlowDto dto = dataFlowService.findOne(new Query());
		dataFlowStateTrigger.setData(dto);
		return dataFlowMachine.handleEvent(dataFlowStateTrigger);
	}

	public StateMachineResult testSubTask(String id, String event, String status){
		return executeAboutSubTask(id, event, userService.loadUserById(toObjectId("61408608c4e5c40012663090")));
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

	public StateMachineResult executeAboutSubTask(String subTaskId, String subTaskEvent, UserDetail userDetail){
		DataFlowEvent event = DataFlowEvent.getEvent(subTaskEvent);
		if (event == null){
			throw new BizException(String.format("SubTask event[%s] does not exist", subTaskEvent));
		}
		return executeAboutSubTask(toObjectId(subTaskId), event , userDetail);

	}

	private StateMachineResult executeAboutSubTask(ObjectId subTaskId, DataFlowEvent event, UserDetail userDetail){
		SubTaskDto dto = subTaskService.findById(subTaskId, userDetail);
		if (dto == null){
			throw new BizException("Task.NotFound");
		}
		return executeAboutSubTask(dto, event, userDetail);

	}

	public StateMachineResult executeAboutSubTask(SubTaskDto dto, DataFlowEvent event, UserDetail userDetail){
		SubTaskStateTrigger trigger = new SubTaskStateTrigger();
		trigger.setSource(SubTaskState.getState(dto.getStatus()));
		trigger.setEvent(event);
		trigger.setUserDetail(userDetail);
		trigger.setData(dto);
		return executeAboutSubTask(trigger);
	}

	private StateMachineResult executeAboutSubTask(SubTaskStateTrigger trigger){
		return subTaskMachine.handleEvent(trigger);
	}
}
