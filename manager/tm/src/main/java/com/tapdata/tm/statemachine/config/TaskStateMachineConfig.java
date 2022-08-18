/**
 * @title: SubTaskStateMachineConfig
 * @description:
 * @author lk
 * @date 2021/11/24
 */
package com.tapdata.tm.statemachine.config;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.statemachine.annotation.EnableStateMachine;
import com.tapdata.tm.statemachine.configuration.AbstractStateMachineConfigurer;
import com.tapdata.tm.statemachine.configuration.StateMachineBuilder;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.TaskState;
import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.TaskStateContext;
import java.util.Date;
import java.util.function.Function;

import com.tapdata.tm.task.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

@EnableStateMachine
public class TaskStateMachineConfig extends AbstractStateMachineConfigurer<TaskState, DataFlowEvent> {


	@Override
	public void configure(StateMachineBuilder<TaskState, DataFlowEvent> builder) {
		builder.transition(TaskState.EDIT, TaskState.SCHEDULING, DataFlowEvent.START)
				.transition(TaskState.SCHEDULING, TaskState.SCHEDULING_FAILED, DataFlowEvent.STOP)
				.transition(TaskState.SCHEDULING, TaskState.SCHEDULING_FAILED, DataFlowEvent.SCHEDULE_FAILED)
				.transition(TaskState.SCHEDULING, TaskState.WAITING_RUN, DataFlowEvent.SCHEDULE_SUCCESS)
				.transition(TaskState.SCHEDULING_FAILED, TaskState.SCHEDULING, DataFlowEvent.START)
				.transition(TaskState.SCHEDULING_FAILED, TaskState.EDIT, DataFlowEvent.EDIT)
				.transition(TaskState.WAITING_RUN, TaskState.SCHEDULING, DataFlowEvent.OVERTIME)
				.transition(TaskState.WAITING_RUN, TaskState.SCHEDULING, DataFlowEvent.SCHEDULE_RESTART)
				.transition(TaskState.WAITING_RUN, TaskState.RUNNING, DataFlowEvent.RUNNING)
				.transition(TaskState.WAITING_RUN, TaskState.STOPPING, DataFlowEvent.STOP)
				.transition(TaskState.RUNNING, TaskState.WAITING_RUN, DataFlowEvent.OVERTIME)
				.transition(TaskState.RUNNING, TaskState.WAITING_RUN, DataFlowEvent.EXIT)
				.transition(TaskState.RUNNING, TaskState.DONE, DataFlowEvent.COMPLETED)
				.transition(TaskState.RUNNING, TaskState.ERROR, DataFlowEvent.ERROR)
				.transition(TaskState.RUNNING, TaskState.STOPPING, DataFlowEvent.STOP)
				.transition(TaskState.ERROR, TaskState.EDIT, DataFlowEvent.EDIT)
				.transition(TaskState.ERROR, TaskState.SCHEDULING, DataFlowEvent.START)
				.transition(TaskState.STOPPING, TaskState.ERROR, DataFlowEvent.ERROR)
				.transition(TaskState.STOPPING, TaskState.DONE, DataFlowEvent.COMPLETED)
				.transition(TaskState.STOPPING, TaskState.STOPPED, DataFlowEvent.STOPPED)
				.transition(TaskState.STOPPING, TaskState.STOPPED, DataFlowEvent.FORCE_STOP)
				.transition(TaskState.STOPPING, TaskState.STOPPED, DataFlowEvent.OVERTIME)
				.transition(TaskState.STOPPED, TaskState.EDIT, DataFlowEvent.EDIT)
				.transition(TaskState.STOPPED, TaskState.SCHEDULING, DataFlowEvent.START)
				.transition(TaskState.DONE, TaskState.EDIT, DataFlowEvent.EDIT)
				.transition(TaskState.DONE, TaskState.SCHEDULING, DataFlowEvent.START);
	}

	@Override
	public Class<? extends StateContext<TaskState, DataFlowEvent>> getContextClass() {
		return TaskStateContext.class;
	}

	@Autowired
	private TaskService taskService;

	public Function<StateContext<TaskState, DataFlowEvent>, StateMachineResult> commonAction() {
		return (stateContext) ->{
			if (stateContext instanceof TaskStateContext){
				Update update = Update.update("status", stateContext.getTarget().getName());
				setOperTime(stateContext.getTarget().getName(), update);
				UpdateResult updateResult = taskService.update(
						Query.query(Criteria.where("_id").is(((TaskStateContext)stateContext).getData().getId())
								.and("status").is(stateContext.getSource().getName())),
						update, stateContext.getUserDetail());
				if (updateResult.wasAcknowledged() && updateResult.getModifiedCount() > 0){
					stateContext.setNeedPostProcessor(true);
				}
				return StateMachineResult.ok(updateResult.getModifiedCount());
			}
			return StateMachineResult.fail("stateContext is not instance of SubTaskStateContext");
		};
	}

	private void setOperTime(String status, Update update){
		Date date = new Date();
		switch (status) {
			case TaskDto.STATUS_WAIT_RUN:  //  scheduled对应startTime和scheduledTime
				update.set("startTime", date).set("scheduledTime", date);
				break;
			case TaskDto.STATUS_STOPPING:  // stopping对应stoppingTime
				update.set("stoppingTime", date);
				break;
			case TaskDto.STATUS_RUNNING:  //  running对应runningTime
				update.set("runningTime", date);
				break;
			case TaskDto.STATUS_ERROR:  //  error对应errorTime和finishTime
				update.set("errorTime", date).set("finishTime", date);
				break;
			default:
				break;
		}
	}
}
