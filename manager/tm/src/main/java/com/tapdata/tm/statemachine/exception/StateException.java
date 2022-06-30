/**
 * @title: StateMachineException
 * @description:
 * @author lk
 * @date 2021/8/5
 */
package com.tapdata.tm.statemachine.exception;

import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.enums.DataFlowState;
import com.tapdata.tm.statemachine.model.StateContext;

public class StateException extends RuntimeException {

	private DataFlowState source;

	private DataFlowState target;

	private DataFlowEvent event;

	public StateException(String msg){
		super(msg);
	}

	public StateException(StateContext context, String msg){
		super(msg);
		if (context != null){
//			this.source = context.getSource();
//			this.target = context.getTarget();
//			this.event = context.getEvent();
		}
	}

	public DataFlowState getSource() {
		return source;
	}

	public DataFlowState getTarget() {
		return target;
	}

	public DataFlowEvent getEvent() {
		return event;
	}
}
