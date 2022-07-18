package io.tapdata.metric.impl;

import io.tapdata.metric.Gauge;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

public class HeapMemoryUsageGauge implements Gauge<Double> {

	private static final String NAME = "HeapMemoryUsage";
	public static final double DEFAULT_MAX = .8;

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public Double getValue() {
		MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
		return 1. * heapMemoryUsage.getUsed() / heapMemoryUsage.getMax();
	}

}
