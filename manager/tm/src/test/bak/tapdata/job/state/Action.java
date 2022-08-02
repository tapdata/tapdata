/**
 * @title: Action
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.job.state;

@FunctionalInterface
public interface Action {

	void execute(StateMachineContext context);
}
