package io.tapdata.metric.impl;

import com.sun.management.OperatingSystemMXBean;
import io.tapdata.metric.DurationGauge;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import java.util.TreeMap;

public class CpuUsageGauge implements DurationGauge<Double> {

	private static final Logger log = LogManager.getLogger(DurationGauge.class);

	private static final String NAME = "CpuUsage";
	private static final Duration BUCKET_DURATION = Duration.ofSeconds(10L);
	private static final Duration WINDOW_DURATION = Duration.ofSeconds(60L);
	public static final double DEFAULT_MAX = .7;

	private volatile Double value = .0;
	private final SlidingWindow slidingWindow = new SlidingWindow();

	public CpuUsageGauge() {

	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public Double getValue() {
		return this.value;
	}

	private synchronized void update() {
		this.value = this.slidingWindow.osMXBean.getProcessCpuLoad();
	}

	@Override
	public void run() {
		this.update();
	}

	@Override
	public Duration getDuration() {
		return BUCKET_DURATION;
	}

	private static class Bucket {
		private final long idx;
		private final long cpuTimeNs;
		private final long sampleTimeNs;

		public Bucket(long idx, long cpuTimeNs, long sampleTimeNs) {
			this.idx = idx;
			this.cpuTimeNs = cpuTimeNs;
			this.sampleTimeNs = sampleTimeNs;
		}

		@Override
		public String toString() {
			return "Bucket{" +
					"idx=" + idx +
					"cpuTimeNs=" + cpuTimeNs +
					", sampleTimeNs=" + sampleTimeNs +
					'}';
		}
	}

	private static class SlidingWindow {

		private final TreeMap<Long, Bucket> bucketMap = new TreeMap<>(Long::compareTo);
		private final int availableProcessors;
		private final long size;
		private final OperatingSystemMXBean osMXBean;
		private final RuntimeMXBean runtimeMXBean;

		public SlidingWindow() {
			this.availableProcessors = Runtime.getRuntime().availableProcessors();
			this.size = WINDOW_DURATION.toMillis() / BUCKET_DURATION.toMillis();
			osMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
			runtimeMXBean = ManagementFactory.getRuntimeMXBean();
			Bucket bucket = getCpuBucket();
			bucketMap.put(bucket.idx, bucket);
		}

		public double diff() {
			Bucket bucket = getCpuBucket();
			bucketMap.putIfAbsent(bucket.idx, bucket);
			if (bucketMap.size() > size) {
				bucketMap.remove(bucketMap.firstKey());
			}
			if (bucketMap.size() <= 1) {
				return .0;
			}
			return this.eval(bucketMap.firstEntry().getValue(), bucketMap.lastEntry().getValue());
		}

		private double eval(Bucket first, Bucket last) {
			long cpuCost = last.cpuTimeNs - first.cpuTimeNs;
			long duration = last.sampleTimeNs - first.sampleTimeNs;
			return 1.0 * cpuCost / (duration * availableProcessors);
		}

		private Bucket getCpuBucket() {
			long cpuTime = osMXBean.getProcessCpuTime();
			long upTime = runtimeMXBean.getUptime() * 1000000;
			long idx = upTime - upTime % BUCKET_DURATION.toNanos();
			return new Bucket(idx, cpuTime, upTime);
		}

	}


}
