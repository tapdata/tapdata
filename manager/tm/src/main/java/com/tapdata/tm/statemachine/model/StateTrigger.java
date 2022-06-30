/**
 * @title: StateMachineTrigger
 * @description:
 * @author lk
 * @date 2021/8/10
 */
package com.tapdata.tm.statemachine.model;

import com.tapdata.tm.config.security.UserDetail;

public abstract class StateTrigger<S, E> {

	private S source;

	private E event;

	private UserDetail userDetail;

	public abstract Object getData();

	public E getEvent() {
		return event;
	}

	public void setEvent(E event) {
		this.event = event;
	}

	public S getSource() {
		return source;
	}

	public void setSource(S source) {
		this.source = source;
	}

	public UserDetail getUserDetail() {
		return userDetail;
	}

	public void setUserDetail(UserDetail userDetail) {
		this.userDetail = userDetail;
	}
}
