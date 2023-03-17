package com.tapdata.entity;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-06-08 18:45
 **/
public class TapdataStartingCdcEvent extends TapdataEvent implements Serializable, Cloneable {
	private static final long serialVersionUID = 5362695192942352471L;

	@Override
	public Object clone() {
		TapdataEvent tapdataEvent = new TapdataStartingCdcEvent();
		super.clone(tapdataEvent);
		return tapdataEvent;
	}
}
