package com.tapdata.entity;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2023-07-20 19:10
 **/
public class TapdataAdjustMemoryEvent extends TapdataEvent implements Serializable {
	private static final long serialVersionUID = -1548217148886674008L;
	public static final int INCREASE = 1;
	public static final int DECREASE = 2;
	public static final int KEEP = 3;
	private int mode;
	private double coefficient;

	public TapdataAdjustMemoryEvent(int mode, double coefficient) {
		this.mode = mode;
		this.coefficient = coefficient;
	}

	public int getMode() {
		return mode;
	}

	public double getCoefficient() {
		return coefficient;
	}

	public boolean needAdjust() {
		return mode != KEEP;
	}

	@Override
	protected void clone(TapdataEvent tapdataEvent) {
		super.clone(tapdataEvent);
		if (tapdataEvent instanceof TapdataAdjustMemoryEvent) {
			TapdataAdjustMemoryEvent tapdataAdjustMemoryEvent = (TapdataAdjustMemoryEvent) tapdataEvent;
			this.mode = tapdataAdjustMemoryEvent.mode;
			this.coefficient = tapdataAdjustMemoryEvent.coefficient;
		}
	}

	@Override
	public String toString() {
		return "TapdataAdjustMemoryEvent{" +
				"mode=" + mode +
				", coefficient=" + coefficient +
				'}';
	}
}
