/**
 * @title: SubTaskStateMachineConfig
 * @description:
 * @author lk
 * @date 2021/11/24
 */
package com.tapdata.tm.statemachine.config;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.statemachine.annotation.EnableStateMachine;
import com.tapdata.tm.statemachine.configuration.AbstractStateMachineConfigurer;
import com.tapdata.tm.statemachine.configuration.StateMachineBuilder;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.SubTaskState;
import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.SubTaskStateContext;
import com.tapdata.tm.task.service.SubTaskService;
import java.util.Date;
import java.util.function.Function;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

@EnableStateMachine
public class SubTaskStateMachineConfig extends AbstractStateMachineConfigurer<SubTaskState, DataFlowEvent> {


	@Override
	public void configure(StateMachineBuilder<SubTaskState, DataFlowEvent> builder) {
		builder.transition(SubTaskState.EDIT, SubTaskState.SCHEDULING, DataFlowEvent.START)
				.transition(SubTaskState.SCHEDULING, SubTaskState.SCHEDULING_FAILED, DataFlowEvent.STOP)
				.transition(SubTaskState.SCHEDULING, SubTaskState.SCHEDULING_FAILED, DataFlowEvent.SCHEDULE_FAILED)
				.transition(SubTaskState.SCHEDULING, SubTaskState.WAITING_RUN, DataFlowEvent.SCHEDULE_SUCCESS)
				.transition(SubTaskState.SCHEDULING_FAILED, SubTaskState.SCHEDULING, DataFlowEvent.START)
				.transition(SubTaskState.SCHEDULING_FAILED, SubTaskState.EDIT, DataFlowEvent.EDIT)
				.transition(SubTaskState.WAITING_RUN, SubTaskState.SCHEDULING, DataFlowEvent.OVERTIME)
				.transition(SubTaskState.WAITING_RUN, SubTaskState.SCHEDULING, DataFlowEvent.SCHEDULE_RESTART)
				.transition(SubTaskState.WAITING_RUN, SubTaskState.RUNNING, DataFlowEvent.RUNNING)
				.transition(SubTaskState.WAITING_RUN, SubTaskState.STOPPING, DataFlowEvent.STOP)
				.transition(SubTaskState.RUNNING, SubTaskState.WAITING_RUN, DataFlowEvent.OVERTIME)
				.transition(SubTaskState.RUNNING, SubTaskState.WAITING_RUN, DataFlowEvent.EXIT)
				.transition(SubTaskState.RUNNING, SubTaskState.DONE, DataFlowEvent.COMPLETED)
				.transition(SubTaskState.RUNNING, SubTaskState.ERROR, DataFlowEvent.ERROR)
				.transition(SubTaskState.RUNNING, SubTaskState.STOPPING, DataFlowEvent.STOP)
				.transition(SubTaskState.ERROR, SubTaskState.EDIT, DataFlowEvent.EDIT)
				.transition(SubTaskState.ERROR, SubTaskState.SCHEDULING, DataFlowEvent.START)
				.transition(SubTaskState.STOPPING, SubTaskState.ERROR, DataFlowEvent.ERROR)
				.transition(SubTaskState.STOPPING, SubTaskState.DONE, DataFlowEvent.COMPLETED)
				.transition(SubTaskState.STOPPING, SubTaskState.STOPPED, DataFlowEvent.STOPPED)
				.transition(SubTaskState.STOPPING, SubTaskState.STOPPED, DataFlowEvent.FORCE_STOP)
				.transition(SubTaskState.STOPPING, SubTaskState.STOPPED, DataFlowEvent.OVERTIME)
				.transition(SubTaskState.STOPPED, SubTaskState.EDIT, DataFlowEvent.EDIT)
				.transition(SubTaskState.STOPPED, SubTaskState.SCHEDULING, DataFlowEvent.START)
				.transition(SubTaskState.DONE, SubTaskState.EDIT, DataFlowEvent.EDIT)
				.transition(SubTaskState.DONE, SubTaskState.SCHEDULING, DataFlowEvent.START);
	}

	@Override
	public Class<? extends StateContext<SubTaskState, DataFlowEvent>> getContextClass() {
		return SubTaskStateContext.class;
	}

	@Autowired
	private SubTaskService subTaskService;

	public Function<StateContext<SubTaskState, DataFlowEvent>, StateMachineResult> commonAction() {
		return (stateContext) ->{
			if (stateContext instanceof SubTaskStateContext){
				Update update = Update.update("status", stateContext.getTarget().getName());
				setOperTime(stateContext.getTarget().getName(), update);
				UpdateResult updateResult = subTaskService.update(
						Query.query(Criteria.where("_id").is(((SubTaskStateContext)stateContext).getData().getId())
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
			case SubTaskDto.STATUS_WAIT_RUN:  //  scheduled对应startTime和scheduledTime
				update.set("startTime", date).set("scheduledTime", date);
				break;
			case SubTaskDto.STATUS_STOPPING:  // stopping对应stoppingTime
				update.set("stoppingTime", date);
				break;
			case SubTaskDto.STATUS_RUNNING:  //  running对应runningTime
				update.set("runningTime", date);
				break;
			case SubTaskDto.STATUS_ERROR:  //  error对应errorTime和finishTime
				update.set("errorTime", date).set("finishTime", date);
				break;
			default:
				break;
		}
	}
}
