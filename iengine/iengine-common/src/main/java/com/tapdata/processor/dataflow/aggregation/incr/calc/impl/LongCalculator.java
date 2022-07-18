package com.tapdata.processor.dataflow.aggregation.incr.calc.impl;

import com.tapdata.processor.dataflow.aggregation.incr.calc.Calculator;

public class LongCalculator implements Calculator<Long> {

	@Override
	public Long add(Long current, Long input) {
		return current + input;
	}

	@Override
	public Long subtract(Long current, Long input) {
		return current - input;
	}

	@Override
	public Long max(Long current, Long input) {
		return Math.max(current, input);
	}

	@Override
	public Long min(Long current, Long input) {
		return Math.min(current, input);
	}

	@Override
	public Long divide(Long sum, Long count) {
		return sum / count;
	}

	@Override
	public boolean eq(Long n1, Long n2) {
		return n1.compareTo(n2) == 0;
	}

	@Override
	public boolean lt(Long n1, Long n2) {
		return n1.compareTo(n2) < 0;
	}

	@Override
	public boolean gt(Long n1, Long n2) {
		return n1.compareTo(n2) > 0;
	}

	@Override
	public Long cast(Number n) {
		return n.longValue();
	}

}
