package com.tapdata.entity;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-05-19 14:57
 **/
public class TapdataHeartbeatEvent extends TapdataEvent implements Serializable, Cloneable {

	private static final long serialVersionUID = -8235448692720473757L;

	public TapdataHeartbeatEvent(Long timestamp, Object offset) {
		setSourceTime(timestamp);
		setStreamOffset(offset);
	}

	@Override
	public Object clone() {
		TapdataEvent tapdataEvent = new TapdataHeartbeatEvent(getSourceTime(), getStreamOffset());
		super.clone(tapdataEvent);
		return tapdataEvent;
	}
}
