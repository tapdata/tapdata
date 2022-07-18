package com.tapdata.processor.dataflow.aggregation.incr.calc;

public interface Calculator<T extends Number> {

	T add(T current, T input);

	T subtract(T current, T input);

	T max(T current, T input);

	T min(T current, T input);

	T divide(T sum, T count);

	boolean eq(T n1, T n2);

	boolean lt(T n1, T n2);

	boolean gt(T n1, T n2);

	T cast(Number n);

}
