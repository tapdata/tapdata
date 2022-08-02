/**
 * @title: SubTaskStateContext
 * @description:
 * @author lk
 * @date 2021/11/25
 */
package com.tapdata.tm.statemachine.model;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.SubTaskState;

public class SubTaskStateContext extends StateContext<SubTaskState, DataFlowEvent>{

	@Override
	public TaskDto getData() {
		return (TaskDto) super.getData();
	}
}
