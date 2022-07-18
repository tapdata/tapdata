package com.tapdata.processor.dataflow.aggregation.incr.calc.impl;

import com.tapdata.processor.dataflow.aggregation.incr.calc.Calculator;

import java.math.BigDecimal;

public class BigDecimalCalculator implements Calculator<BigDecimal> {

	@Override
	public BigDecimal add(BigDecimal current, BigDecimal input) {
		return current.add(input);
	}

	@Override
	public BigDecimal subtract(BigDecimal current, BigDecimal input) {
		return current.subtract(input);
	}

	@Override
	public BigDecimal max(BigDecimal current, BigDecimal input) {
		return current.compareTo(input) < 0 ? input : current;
	}

	@Override
	public BigDecimal min(BigDecimal current, BigDecimal input) {
		return current.compareTo(input) > 0 ? input : current;
	}

	@Override
	public BigDecimal divide(BigDecimal sum, BigDecimal count) {
		return sum.divide(count, BigDecimal.ROUND_HALF_EVEN);
	}

	@Override
	public boolean eq(BigDecimal n1, BigDecimal n2) {
		return n1.compareTo(n2) == 0;
	}

	@Override
	public boolean lt(BigDecimal n1, BigDecimal n2) {
		return n1.compareTo(n2) < 0;
	}

	@Override
	public boolean gt(BigDecimal n1, BigDecimal n2) {
		return n1.compareTo(n2) > 0;
	}

	@Override
	public BigDecimal cast(Number n) {
		return BigDecimal.valueOf(n.doubleValue());
	}

}
