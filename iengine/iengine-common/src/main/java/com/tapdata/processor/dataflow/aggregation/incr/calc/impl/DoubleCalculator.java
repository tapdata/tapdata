package com.tapdata.processor.dataflow.aggregation.incr.calc.impl;

import com.tapdata.processor.dataflow.aggregation.incr.calc.Calculator;

public class DoubleCalculator implements Calculator<Double> {

	@Override
	public Double add(Double current, Double input) {
		return current + input;
	}

	@Override
	public Double subtract(Double current, Double input) {
		return current - input;
	}

	@Override
	public Double max(Double current, Double input) {
		return Math.max(current, input);
	}

	@Override
	public Double min(Double current, Double input) {
		return Math.min(current, input);
	}

	@Override
	public Double divide(Double sum, Double count) {
		return sum / count;
	}

	@Override
	public boolean eq(Double n1, Double n2) {
		return n1.compareTo(n2) == 0;
	}

	@Override
	public boolean lt(Double n1, Double n2) {
		return n1.compareTo(n2) < 0;
	}

	@Override
	public boolean gt(Double n1, Double n2) {
		return n1.compareTo(n2) > 0;
	}

	@Override
	public Double cast(Number n) {
		return n.doubleValue();
	}

}
