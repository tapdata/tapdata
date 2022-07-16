package io.tapdata.flow.engine.V2.entity;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-05-19 16:06
 **/
public class TapdataCompleteSnapshotEvent extends TapdataEvent implements Serializable, Cloneable {
	private static final long serialVersionUID = 5717006177280281597L;

	@Override
	public Object clone() {
		TapdataEvent tapdataEvent = new TapdataCompleteSnapshotEvent();
		super.clone(tapdataEvent);
		return tapdataEvent;
	}
}
