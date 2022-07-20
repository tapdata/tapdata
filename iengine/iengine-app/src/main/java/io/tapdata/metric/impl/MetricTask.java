package io.tapdata.metric.impl;


import io.tapdata.metric.DurationGauge;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricTask implements Runnable {

	private static final Logger log = LogManager.getLogger(MetricTask.class);

	private final ScheduledExecutorService scheduledExecutorService;
	private final DurationGauge<?> durationGauge;

	public MetricTask(ScheduledExecutorService scheduledExecutorService, DurationGauge<?> durationGauge) {
		this.scheduledExecutorService = scheduledExecutorService;
		this.durationGauge = durationGauge;
	}

	@Override
	public void run() {
		try {
			this.durationGauge.run();
		} catch (Throwable t) {
			log.error(String.format("metric [%s] update value error ", this.durationGauge.getName()), t);
		} finally {
			scheduledExecutorService.schedule(this, durationGauge.getDuration().toMillis(), TimeUnit.MILLISECONDS);
		}
	}

}
