/**
 * @title: DataFlowStateMachineActionConfig
 * @description:
 * @author lk
 * @date 2021/11/30
 */
package com.tapdata.tm.statemachine.config;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.statemachine.annotation.OnAction;
import com.tapdata.tm.statemachine.annotation.WithStateMachine;
import com.tapdata.tm.statemachine.enums.Transitions;
import com.tapdata.tm.statemachine.model.DataFlowStateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import java.util.Date;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
@WithStateMachine
public class DataFlowStateMachineActionConfig {

	@Autowired
	private DataFlowService dataFlowService;

	@Autowired
	private WorkerService workerService;

	@Autowired
	private SettingsService settingsService;

//	@OnAction(Transitions.SUBTASK_SCHEDULE_SUCEESS)
//	public StateMachineResult scheduleSuccess(DataFlowStateContext context){
//		DataFlowDto dataFlowDto = context.getData();
//		if (StringUtils.isBlank(dataFlowDto.getAgentId())){
//			throw new BizException("Agent.Not.Found");
//		}
//		Date date = new Date();
//		Update update = Update.update("status", context.getTarget().getName()).set("agentId", dataFlowDto.getAgentId())
//				.set("startTime", date).set("scheduledTime", date);
//		UpdateResult result = dataFlowService.update(Query.query(Criteria.where("_id").is(dataFlowDto.getId())
//						.and("status").is(context.getSource().getName())),
//				update, context.getUserDetail());
//
//		return StateMachineResult.ok(result.getModifiedCount());
//	}

	/**
	 * 企业版:  运行中会重新分配agentId, 分配后变为调度中
	 * 云版:    运行中会变为调度中,不会分配agentId,
	 **/
	@OnAction(Transitions.DATAFLOW_OVERTIME)
	public StateMachineResult overtime(DataFlowStateContext context){
		DataFlowDto dataFlowDto = context.getData();
		UserDetail userDetail = context.getUserDetail();

		Object buildProfile = settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.BUILD_PROFILE);
		boolean isCloud = "CLOUD".equals(buildProfile) || "DRS".equals(buildProfile) || "DFS".equals(buildProfile);

		String processId = null;
		if (!isCloud){
			dataFlowDto.setAgentId(null);
			workerService.scheduleTaskToEngine(dataFlowDto, userDetail);
			processId = dataFlowDto.getAgentId();

			if (StringUtils.isBlank(processId)){
				return StateMachineResult.fail("ProcessId is blank");
			}
		}

		Update update = Update.update("status", context.getTarget().getName());
		if (StringUtils.isNotBlank(processId)){
			update.set("agentId", processId).set("pingTime", System.currentTimeMillis());
		}
		UpdateResult updateResult = dataFlowService.update(Query.query(Criteria.where("_id").is(dataFlowDto.getId())
						.and("status").is(context.getSource().getName())),
				update, context.getUserDetail());

		return StateMachineResult.ok(updateResult.getModifiedCount());
	}
}
