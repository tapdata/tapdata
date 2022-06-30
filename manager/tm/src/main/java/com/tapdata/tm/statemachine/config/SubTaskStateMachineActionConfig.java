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
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.statemachine.annotation.OnAction;
import com.tapdata.tm.statemachine.annotation.WithStateMachine;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.Transitions;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.SubTaskStateContext;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.SubTaskService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.worker.service.WorkerService;
import java.util.Date;

import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
@WithStateMachine
@Setter(onMethod_ = {@Autowired})
public class SubTaskStateMachineActionConfig {

	private SubTaskService subTaskService;
	private StateMachineService stateMachineService;
	private WorkerService workerService;
	private TaskService taskService;

	@OnAction(Transitions.SUBTASK_START)
	public StateMachineResult start(SubTaskStateContext context){
		SubTaskDto subTaskDto = context.getData();
		subTaskService.start(subTaskDto.getId(), context.getUserDetail());
		return StateMachineResult.ok();
	}

	@OnAction(Transitions.SUBTASK_SCHEDULE_SUCEESS)
	public StateMachineResult scheduleSuccess(SubTaskStateContext context){
		SubTaskDto subTaskDto = context.getData();
		if (StringUtils.isBlank(subTaskDto.getAgentId())){
			throw new BizException("Agent.Not.Found");
		}
		Date date = new Date();
		Update update = Update.update("status", context.getTarget().getName()).set("agentId", subTaskDto.getAgentId())
				.set("startTime", date).set("scheduledTime", date);
		UpdateResult result = subTaskService.update(Query.query(Criteria.where("_id").is(subTaskDto.getId())
						.and("status").is(context.getSource().getName())),
				update, context.getUserDetail());

		return StateMachineResult.ok(result.getModifiedCount());
	}

	@OnAction(Transitions.SUBTASK_OVERTIME)
	public StateMachineResult overtime(SubTaskStateContext context){
		SubTaskDto subTaskDto = context.getData();
		Update update = Update.update("status", context.getTarget().getName());
		UpdateResult updateResult = subTaskService.update(Query.query(Criteria.where("_id").is(subTaskDto.getId())
						.and("status").is(context.getSource().getName())),
				update, context.getUserDetail());
		StateMachineResult result = null;
		if (updateResult.wasAcknowledged() && updateResult.getModifiedCount() > 0){
			SubTaskDto dto = subTaskService.findById(subTaskDto.getId(), context.getUserDetail());
			if (dto != null){
				// subtask reschedule
				ObjectId parentId = dto.getParentId();
				TaskDto taskDto = taskService.findById(parentId);
				assert taskDto != null;
				if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), taskDto.getAccessNodeType())
						&& CollectionUtils.isNotEmpty(taskDto.getAccessNodeProcessIdList())) {
					dto.setAgentId(taskDto.getAccessNodeProcessIdList().get(0));
				} else {
					dto.setAgentId(null);
				}

				workerService.scheduleTaskToEngine(dto, context.getUserDetail(), "SubTask", dto.getName());
				if (StringUtils.isNotBlank(dto.getAgentId())){
					result = stateMachineService.executeAboutSubTask(dto, DataFlowEvent.SCHEDULE_SUCCESS, context.getUserDetail());
				}else {
					result = stateMachineService.executeAboutSubTask(dto, DataFlowEvent.SCHEDULE_FAILED, context.getUserDetail());
				}
			}else {
				result = stateMachineService.executeAboutSubTask(subTaskDto, DataFlowEvent.SCHEDULE_FAILED, context.getUserDetail());
			}

		}else {
			result = StateMachineResult.ok(updateResult.getModifiedCount());
		}

		return result;
	}
}
