/**
 * @title: StateMachine
 * @description:
 * @author lk
 * @date 2021/11/12
 */
package com.tapdata.tm.statemachine;

import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.model.StateTrigger;

public interface StateMachine<S, E> {

	void initStateTransition();

	StateMachineResult handleEvent(StateTrigger<S, E> trigger);

}
