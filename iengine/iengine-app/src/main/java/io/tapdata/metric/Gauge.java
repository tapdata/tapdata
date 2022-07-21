package io.tapdata.metric;

public interface Gauge<T> {

	String getName();

	T getValue();

}
