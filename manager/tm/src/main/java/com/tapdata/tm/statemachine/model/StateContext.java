/**
 * @title: StateContext
 * @description:
 * @author lk
 * @date 2021/7/29
 */
package com.tapdata.tm.statemachine.model;

import com.tapdata.tm.config.security.UserDetail;

public class StateContext<S, E> {

	public S source;

	public S target;

	private E event;

	private Boolean needPostProcessor;

	private UserDetail userDetail;

	private Object data;

	public StateContext() {
	}

	public StateContext(StateTrigger<S, E> trigger) {
		this.source = trigger.getSource();
		this.event = trigger.getEvent();
		this.userDetail = trigger.getUserDetail();
		this.data = trigger.getData();
	}

	public S getSource() {
		return source;
	}

	public S getTarget() {
		return target;
	}

	public void setTarget(S target) {
		this.target = target;
	}

	public E getEvent() {
		return event;
	}

	public void setEvent(E event) {
		this.event = event;
	}

	public Boolean getNeedPostProcessor() {
		return needPostProcessor;
	}

	public void setNeedPostProcessor(Boolean needPostProcessor) {
		this.needPostProcessor = needPostProcessor;
	}

	public UserDetail getUserDetail() {
		return userDetail;
	}

	public void setUserDetail(UserDetail userDetail) {
		this.userDetail = userDetail;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public void setSource(S source) {
		this.source = source;
	}
}
