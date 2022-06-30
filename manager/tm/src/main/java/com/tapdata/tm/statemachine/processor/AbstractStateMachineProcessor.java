/**
 * @title: AbstractStateMachineProcessor
 * @description:
 * @author lk
 * @date 2021/8/5
 */
package com.tapdata.tm.statemachine.processor;

import com.tapdata.tm.statemachine.Processor;
import com.tapdata.tm.statemachine.model.StateContext;

public abstract class AbstractStateMachineProcessor implements Processor{

	@Override
	public boolean evaluate(StateContext context) {
		return true;
	}

	@Override
	public void executeAfter(StateContext context) {

	}

}
