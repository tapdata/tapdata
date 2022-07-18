package io.tapdata.metric;

import io.tapdata.common.SettingService;
import io.tapdata.metric.impl.CpuUsageGauge;
import io.tapdata.metric.impl.HeapMemoryUsageGauge;
import io.tapdata.metric.impl.MetricTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class MetricManager {

	private static final Logger log = LogManager.getLogger(MetricManager.class);

	private SettingWrapper settingWrapper;
	private final Map<Predicate, Supplier<?>> predicateMap = new HashMap<>();
	private final Set<Gauge<?>> gaugeSet = new HashSet<>();

	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

	public MetricManager(SettingService settingService) {
		this.settingWrapper = new SettingWrapper(settingService);
	}

	public void init() {
		final CpuUsageGauge cpuUsageGauge = new CpuUsageGauge();
		final HeapMemoryUsageGauge heapMemoryUsageGauge = new HeapMemoryUsageGauge();

		gaugeSet.add(cpuUsageGauge);
		gaugeSet.add(heapMemoryUsageGauge);

		for (Gauge<?> gauge : gaugeSet) {
			if (gauge instanceof DurationGauge) {
				DurationGauge<?> durationGauge = (DurationGauge<?>) gauge;
				MetricTask metricTask = new MetricTask(this.scheduledExecutorService, durationGauge);
				scheduledExecutorService.schedule(metricTask, durationGauge.getDuration().toMillis(), TimeUnit.MILLISECONDS);
			}
		}

		// temporary disable the cpu limit
		// predicateMap.put(new MaxPredicate<>(cpuUsageGauge), () -> this.settingWrapper.getDouble("maxCpuUsage", CpuUsageGauge.DEFAULT_MAX));
		// temporary disable the memory limit
		// predicateMap.put(new MaxPredicate<>(heapMemoryUsageGauge), () -> this.settingWrapper.getDouble("maxHeapMemoryUsage", HeapMemoryUsageGauge.DEFAULT_MAX));
	}

	public PredicateResult test() {
		for (Map.Entry<Predicate, Supplier<?>> e : this.predicateMap.entrySet()) {
			PredicateResult result = e.getKey().test(e.getValue().get());
			if (!result.isHealth()) {
				return result;
			}
		}
		return new PredicateResult(true, "everything is good");
	}

	public Map<String, Object> getValueMap() {
		return gaugeSet.stream().collect(Collectors.toMap(Gauge::getName, Gauge::getValue));
	}
}
