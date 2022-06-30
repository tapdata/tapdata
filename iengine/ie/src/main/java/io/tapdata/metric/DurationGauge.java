package io.tapdata.metric;

import java.time.Duration;

public interface DurationGauge<T> extends Gauge<T>, Runnable {

	Duration getDuration();

}
