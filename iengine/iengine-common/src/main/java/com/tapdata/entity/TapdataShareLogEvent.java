package com.tapdata.entity;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-06-15 11:41
 **/
public class TapdataShareLogEvent extends TapdataEvent implements Serializable, Cloneable {

	private static final long serialVersionUID = 5639631218965384484L;

	@Override
	public Object clone() {
		TapdataEvent tapdataEvent = new TapdataShareLogEvent();
		super.clone(tapdataEvent);
		return tapdataEvent;
	}
}
