/**
 * @title: Gurad
 * @description:
 * @author lk
 * @date 2021/7/30
 */
package com.tapdata.job.state;

@FunctionalInterface
public interface Gurad {

	boolean evaluate(StateMachineContext context);
}
