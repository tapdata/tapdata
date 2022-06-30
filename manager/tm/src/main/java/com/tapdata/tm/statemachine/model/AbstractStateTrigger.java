/**
 * @title: StateTrigger1
 * @description:
 * @author lk
 * @date 2021/11/25
 */
package com.tapdata.tm.statemachine.model;

public abstract class AbstractStateTrigger<S, E, T> extends StateTrigger<S, E> {

	private T data;

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}
}
