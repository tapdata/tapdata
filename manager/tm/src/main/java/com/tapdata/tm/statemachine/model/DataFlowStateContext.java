/**
 * @title: DataFlowStateContext
 * @description:
 * @author lk
 * @date 2021/11/15
 */
package com.tapdata.tm.statemachine.model;

import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.DataFlowState;

public class DataFlowStateContext extends StateContext<DataFlowState, DataFlowEvent> {

	public DataFlowDto getData() {
		return (DataFlowDto) super.getData();
	}

}
