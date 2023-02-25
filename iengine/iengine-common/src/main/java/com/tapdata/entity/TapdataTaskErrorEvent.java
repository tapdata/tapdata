package com.tapdata.entity;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-10-27 11:32
 **/
public class TapdataTaskErrorEvent extends TapdataEvent implements Serializable, Cloneable {

	private static final long serialVersionUID = -114543737789067621L;
	private Throwable throwable;

	public TapdataTaskErrorEvent(Throwable throwable) {
		this.throwable = throwable;
	}

	public Throwable getThrowable() {
		return throwable;
	}

	public void setThrowable(Throwable throwable) {
		this.throwable = throwable;
	}

	@Override
	public Object clone() {
		TapdataEvent tapdataEvent = new TapdataTaskErrorEvent(throwable);
		super.clone(tapdataEvent);
		return tapdataEvent;
	}
}
