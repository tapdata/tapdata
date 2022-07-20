package io.tapdata.metric;

public interface Predicate<T> {

	PredicateResult test(T v);

}
