/**
 * @title: Action
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.tm.statemachine;

import com.tapdata.tm.statemachine.model.StateContext;
import com.tapdata.tm.statemachine.model.StateMachineResult;

@FunctionalInterface
public interface Action {

	StateMachineResult execute(StateContext context);
}
