/**
 * @title: Processor
 * @description:
 * @author lk
 * @date 2021/8/5
 */
package com.tapdata.tm.statemachine;

import com.tapdata.tm.statemachine.model.Transition;

public interface Processor extends Guard, Action, AfterAction {

	default <S, E> void configure(Transition<S, E> transition){
		transition.guard(this::evaluate).action(this::execute).afterAction(this::executeAfter);
	};
}
