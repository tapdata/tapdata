/**
 * @title: AfterAction
 * @description:
 * @author lk
 * @date 2021/8/5
 */
package com.tapdata.tm.statemachine;

import com.tapdata.tm.statemachine.model.StateContext;

@FunctionalInterface
public interface AfterAction {

	void executeAfter(StateContext context);
}
