/**
 * @title: SubTaskStateMachineService
 * @description:
 * @author lk
 * @date 2021/11/25
 */
package com.tapdata.tm.statemachine.config;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.statemachine.annotation.OnAction;
import com.tapdata.tm.statemachine.annotation.WithStateMachine;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.Transitions;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.TaskStateContext;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.worker.service.WorkerService;
import java.util.Date;

import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
@WithStateMachine
@Setter(onMethod_ = {@Autowired})
public class TaskStateMachineActionConfig {

	private StateMachineService stateMachineService;
	private WorkerService workerService;
	private TaskService taskService;

//	@OnAction(Transitions.SUBTASK_START)
//	public StateMachineResult start(TaskStateContext context){
//		TaskDto subTaskDto = context.getData();
//		taskService.runMachine(subTaskDto, context.getUserDetail());
//		return StateMachineResult.ok();
//	}

//	@OnAction(Transitions.SUBTASK_SCHEDULE_SUCEESS)
//	public StateMachineResult scheduleSuccess(TaskStateContext context){
//		TaskDto taskDto = context.getData();
//		if (StringUtils.isBlank(taskDto.getAgentId())){
//			throw new BizException("Agent.Not.Found");
//		}
//		Date date = new Date();
//		Update update = Update.update("status", context.getTarget().getName()).set("agentId", taskDto.getAgentId())
//				.set("startTime", date).set("scheduledTime", date);
//		UpdateResult result = taskService.update(Query.query(Criteria.where("_id").is(taskDto.getId())
//						.and("status").is(context.getSource().getName())),
//				update, context.getUserDetail());
//
//		return StateMachineResult.ok(result.getModifiedCount());
//	}
//
//	@OnAction(Transitions.SUBTASK_OVERTIME)
//	public StateMachineResult overtime(TaskStateContext context){
//		TaskDto taskDto = context.getData();
//		Update update = Update.update("status", context.getTarget().getName());
//		UpdateResult updateResult = taskService.update(Query.query(Criteria.where("_id").is(taskDto.getId())
//						.and("status").is(context.getSource().getName())),
//				update, context.getUserDetail());
//		StateMachineResult result = null;
//		if (updateResult.wasAcknowledged() && updateResult.getModifiedCount() > 0){
//			TaskDto dto = taskService.findById(taskDto.getId(), context.getUserDetail());
//			if (taskService != null){
//				// subtask reschedule
//				assert dto != null;
//				if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), dto.getAccessNodeType())
//						&& CollectionUtils.isNotEmpty(dto.getAccessNodeProcessIdList())) {
//					dto.setAgentId(dto.getAccessNodeProcessIdList().get(0));
//				} else {
//					dto.setAgentId(null);
//				}
//
//				workerService.scheduleTaskToEngine(dto, context.getUserDetail(), "SubTask", dto.getName());
//				if (StringUtils.isNotBlank(dto.getAgentId())){
//					result = stateMachineService.executeAboutTask(dto, DataFlowEvent.SCHEDULE_SUCCESS, context.getUserDetail());
//				}else {
//					result = stateMachineService.executeAboutTask(dto, DataFlowEvent.SCHEDULE_FAILED, context.getUserDetail());
//				}
//			}else {
//				result = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, context.getUserDetail());
//			}
//
//		}else {
//			result = StateMachineResult.ok(updateResult.getModifiedCount());
//		}
//
//		return result;
//	}
}
