package com.tapdata.processor.dataflow.aggregation.incr.calc;

import com.tapdata.processor.dataflow.aggregation.incr.calc.impl.BigDecimalCalculator;
import com.tapdata.processor.dataflow.aggregation.incr.calc.impl.DoubleCalculator;
import com.tapdata.processor.dataflow.aggregation.incr.calc.impl.LongCalculator;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ComposeCalculator implements Calculator<Number> {

	private static final ComposeCalculator INSTANCE = new ComposeCalculator();

	private static final Set<Class<?>> INTEGER_SET = new HashSet<Class<?>>() {{
		this.add(byte.class);
		this.add(short.class);
		this.add(int.class);
		this.add(long.class);
		this.add(Byte.class);
		this.add(Short.class);
		this.add(Integer.class);
		this.add(Long.class);
	}};
	private static final Set<Class<?>> FLOAT_SET = new HashSet<Class<?>>() {{
		this.add(float.class);
		this.add(double.class);
		this.add(Float.class);
		this.add(Double.class);
	}};


	private final Map<DataType, Calculator<? extends Number>> calculatorMap = new HashMap<>();

	private ComposeCalculator() {
		calculatorMap.put(DataType.INTEGER, new LongCalculator());
		calculatorMap.put(DataType.FLOAT, new DoubleCalculator());
		calculatorMap.put(DataType.BIG_DECIMAL, new BigDecimalCalculator());
	}

	public static ComposeCalculator getInstance() {
		return INSTANCE;
	}

	@Override
	public Number add(Number current, Number input) {
		return this.execute(current, input, Calculator::add);
	}

	@Override
	public Number subtract(Number current, Number input) {
		return this.execute(current, input, Calculator::subtract);
	}

	@Override
	public Number max(Number current, Number input) {
		return this.execute(current, input, Calculator::max);
	}

	@Override
	public Number min(Number current, Number input) {
		return this.execute(current, input, Calculator::min);
	}

	@Override
	public Number divide(Number sum, Number count) {
		return this.execute(sum, count, Calculator::divide);
	}

	@Override
	public boolean eq(Number n1, Number n2) {
		return this.execute(n1, n2, Calculator::eq);
	}

	@Override
	public boolean lt(Number n1, Number n2) {
		return this.execute(n1, n2, Calculator::lt);
	}

	@Override
	public boolean gt(Number n1, Number n2) {
		return this.execute(n1, n2, Calculator::gt);
	}

	@Override
	public Number cast(Number n) {
		return calculatorMap.get(detect(n)).cast(n);
	}

	private <R> R execute(Number n1, Number n2, CalcExecutor<R> executor) {
		final DataType dataType = this.detect(n1, n2);
		Calculator<? extends Number> calculator = calculatorMap.get(dataType);
		return executor.exec(calculator, calculator.cast(n1), calculator.cast(n2));
	}

	private DataType detect(Number n1) {
		if (n1 instanceof BigDecimal) {
			return DataType.BIG_DECIMAL;
		}
		if (FLOAT_SET.contains(n1.getClass())) {
			return DataType.FLOAT;
		}
		if (INTEGER_SET.contains(n1.getClass())) {
			return DataType.INTEGER;
		}
		throw new IllegalArgumentException(String.format("unsupported data type: %s", n1.getClass()));
	}

	private DataType detect(Number n1, Number n2) {
		if (n1 instanceof BigDecimal || n2 instanceof BigDecimal) {
			return DataType.BIG_DECIMAL;
		}
		if (FLOAT_SET.contains(n1.getClass()) || FLOAT_SET.contains(n2.getClass())) {
			return DataType.FLOAT;
		}
		if (INTEGER_SET.contains(n1.getClass()) || INTEGER_SET.contains(n2.getClass())) {
			return DataType.INTEGER;
		}
		throw new IllegalArgumentException(String.format("unsupported data type: %s", n1.getClass()));
	}

}
