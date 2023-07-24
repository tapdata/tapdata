package io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2023-07-24 10:52
 **/
public class DynamicAdjustResult implements Serializable {
	private static final long serialVersionUID = 4321665506497007324L;
	private Mode mode;
	private double coefficient;

	public DynamicAdjustResult() {
		this.mode = Mode.KEEP;
	}

	public DynamicAdjustResult(Mode mode, double coefficient) {
		this.mode = mode;
		this.coefficient = coefficient;
	}

	public Mode getMode() {
		return mode;
	}

	public double getCoefficient() {
		return coefficient;
	}

	public enum Mode {
		INCREASE(1),
		DECREASE(2),
		KEEP(3),
		;
		private final int value;

		Mode(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	}
}
