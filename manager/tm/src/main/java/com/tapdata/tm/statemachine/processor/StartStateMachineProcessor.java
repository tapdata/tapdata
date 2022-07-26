/**
 * @title: DefaultStateMachineProcessor
 * @description:
 * @author lk
 * @date 2021/8/5
 */
package com.tapdata.tm.statemachine.processor;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.CustomerJobLogs.CustomerJobLog;
import com.tapdata.tm.CustomerJobLogs.service.CustomerJobLogsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.behavior.BehaviorCode;
import com.tapdata.tm.behavior.service.BehaviorService;
import com.tapdata.tm.commons.websocket.MessageInfoBuilder;
import com.tapdata.tm.commons.websocket.ReturnCallback;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.dto.DataFlowStatus;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.dataflowrecord.dto.DataFlowRecordDto;
import com.tapdata.tm.dataflowrecord.service.DataFlowRecordService;
import com.tapdata.tm.log.dto.LogDto;
import com.tapdata.tm.log.service.LogService;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.statemachine.annotation.StateMachineHandler;
import com.tapdata.tm.statemachine.enums.Transitions;
import com.tapdata.tm.statemachine.model.DataFlowStateContext;
import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import java.util.Date;
import java.util.HashMap;

import com.tapdata.tm.ws.endpoint.WebSocketManager;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

@StateMachineHandler(transitions = Transitions.DATAFLOW_START)
@Slf4j
public class StartStateMachineProcessor extends AbstractStateMachineProcessor {

	private final DataFlowService dataFlowService;

	private final WorkerService workerService;

	private final CustomerJobLogsService customerJobLogsService;

	private final DataFlowRecordService dataFlowRecordService;

	@Autowired
	private MessageQueueService messageQueueService;

	@Autowired
	private BehaviorService behaviorService;

	public StartStateMachineProcessor(DataFlowService dataFlowService, WorkerService workerService, CustomerJobLogsService customerJobLogsService, DataFlowRecordService dataFlowRecordService) {
		this.dataFlowService = dataFlowService;
		this.workerService = workerService;
		this.customerJobLogsService = customerJobLogsService;
		this.dataFlowRecordService = dataFlowRecordService;
	}

	@Override
	public StateMachineResult execute(StateContext stateContext) {
		DataFlowStateContext context = (DataFlowStateContext)stateContext;
		DataFlowDto dataFlowDto = context.getData();
		UserDetail userDetail = stateContext.getUserDetail();
		workerService.scheduleTaskToEngine(dataFlowDto, userDetail);
		String processId = dataFlowDto.getAgentId();
		CustomerJobLog dataFlowLog = new CustomerJobLog(dataFlowDto.getId().toString(),dataFlowDto.getName(), CustomerJobLogsService.DataFlowType.clone);
		if (StringUtils.isBlank(processId)){
			customerJobLogsService.noAvailableAgents(dataFlowLog, userDetail);
			throw new BizException("Agent.Not.Found");
		}
		WorkerDto workerDto = workerService.findOne(new Query(Criteria.where("process_id").is(processId)));
		dataFlowLog.setAgentHost(workerDto.getHostname());
		customerJobLogsService.assignAgent(dataFlowLog, userDetail);
		dataFlowService.transformSchema(dataFlowDto, userDetail);
		Date date = new Date();
		// 增量任务只会自动运行一次，如果运行过就不会再自动运行了
		if (dataFlowDto.getSetting() != null && "initial_sync".equals(dataFlowDto.getSetting().get("sync_type"))){
			dataFlowService.setNextScheduleTime(dataFlowDto);
		}else {
			dataFlowDto.setNextScheduledTime(null);
		}
		dataFlowDto.setStartTime(date);
		dataFlowDto.setFinishTime(null);
		dataFlowDto.setDataFlowRecordId(null);
		DataFlowRecordDto dataFlowRecordDto = dataFlowRecordService.saveRecord(dataFlowDto, DataFlowRecordDto.STATUS_RUNNING, userDetail);
		UpdateResult update = dataFlowService.update(Query.query(Criteria.where("id").is(dataFlowDto.getId()).and("status").is(context.getSource().getName())),
				Update.update("status", context.getTarget().getName())
						.set("agentId", processId)
						.set("startTime", date)
						.set("scheduledTime", date)
						.set("errorTime", null)
						.set("pausedTime", null)
						.set("finishTime", null)
						.set("startType", dataFlowDto.getStartType())
						.set("dataFlowRecordId", dataFlowRecordDto.getId().toHexString())
						.set("nextScheduledTime", dataFlowDto.getNextScheduledTime()), userDetail);
		if (update.wasAcknowledged() && update.getModifiedCount() > 0){
			context.setNeedPostProcessor(true);
		}

		try {
			messageQueueService.sendMessage(processId, MessageInfoBuilder.newMessage()
					.call("dataFlowScheduler", "scheduledDataFlow")
					.body(dataFlowDto.getId().toHexString()).build());
		} catch (Exception e) {
			log.error("Send message to agent failed", e);
		}

		behaviorService.trace(dataFlowDto, userDetail, BehaviorCode.startDataFlow);

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
