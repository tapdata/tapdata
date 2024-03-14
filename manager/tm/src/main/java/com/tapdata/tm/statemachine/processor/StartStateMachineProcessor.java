/**
 * @title: DefaultStateMachineProcessor
 * @description:
 * @author lk
 * @date 2021/8/5
 */
package com.tapdata.tm.statemachine.processor;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.statemachine.annotation.StateMachineHandler;
import com.tapdata.tm.statemachine.enums.Transitions;
import com.tapdata.tm.statemachine.model.DataFlowStateContext;
import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.service.WorkerService;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

@StateMachineHandler(transitions = Transitions.DATAFLOW_START)
@Slf4j
public class StartStateMachineProcessor extends AbstractStateMachineProcessor {

	private final DataFlowService dataFlowService;

	private final WorkerService workerService;

	private final MessageService messageService;

	public StartStateMachineProcessor(DataFlowService dataFlowService, WorkerService workerService, MessageService messageService, UserService userService) {
		this.dataFlowService = dataFlowService;
		this.workerService = workerService;
		this.messageService=messageService;
	}

	@Override
	public StateMachineResult execute(StateContext stateContext) {
		DataFlowStateContext context = (DataFlowStateContext)stateContext;
		DataFlowDto dataFlowDto = context.getData();
		UserDetail userDetail = stateContext.getUserDetail();
		workerService.scheduleTaskToEngine(dataFlowDto, userDetail);
		String processId = dataFlowDto.getAgentId();
		if (StringUtils.isBlank(processId)){
			throw new BizException("Agent.Not.Found");
		}
		dataFlowService.transformSchema(dataFlowDto, userDetail);
		Date date = new Date();
		UpdateResult update = dataFlowService.update(Query.query(Criteria.where("id").is(dataFlowDto.getId()).and("status").is(context.getSource().getName())),
				Update.update("status", context.getTarget().getName()).set("agentId", processId).set("startTime", date).set("scheduledTime", date), userDetail);
		if (update.wasAcknowledged() && update.getModifiedCount() > 0){
			context.setNeedPostProcessor(true);
		}
		return StateMachineResult.ok(update.getModifiedCount());
	}

	/**
	 * 任务启动
	 * @param stateContext
	 */
	@Override
	public void executeAfter(StateContext stateContext) {
//		log.info("任务启动 stateContext：{} ", JsonUtil.toJson(stateContext));
//		UserDetail userDetail = stateContext.getUserDetail();
//		DataFlowStateContext context = (DataFlowStateContext)stateContext;
//		DataFlowDto dataFlowDto = context.getData();
//		messageService.add(dataFlowDto.getName(),dataFlowDto.getId().toString(), MsgTypeEnum.CONNECTED, SystemEnum.DATAFLOW,dataFlowDto.getId().toHexString(),userDetail);
	}
}
