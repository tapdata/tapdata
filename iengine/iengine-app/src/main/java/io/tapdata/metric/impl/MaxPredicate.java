package io.tapdata.metric.impl;

import io.tapdata.metric.Gauge;
import io.tapdata.metric.Predicate;
import io.tapdata.metric.PredicateResult;

public class MaxPredicate<T extends Comparable<T>> implements Predicate<T> {

	private final Gauge<T> gauge;

	private static final String OVERLOAD_FORMMAT = "metric [%s] is overload, current: %s, max: %s";
	private static final String HEALTH_FORMMAT = "metric [%s] is health, current: %s, max: %s";

	public MaxPredicate(Gauge<T> gauge) {
		this.gauge = gauge;
	}

	@Override
	public PredicateResult test(T maxValue) {
		T gaugeValue = this.gauge.getValue();
		return gaugeValue.compareTo(maxValue) >= 0 ? new PredicateResult(false, formatDetail(OVERLOAD_FORMMAT, gaugeValue, maxValue)) : new PredicateResult(true, formatDetail(HEALTH_FORMMAT, gaugeValue, maxValue));
	}

	private String formatDetail(String formatter, T gaugeValue, T maxValue) {
		return String.format(formatter, this.gauge.getName(), gaugeValue, maxValue);
	}

}
