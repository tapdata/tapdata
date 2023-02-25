package io.tapdata.flow.engine.V2.monitor;

import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections.CollectionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-03-02 02:15
 **/
public class MonitorManager implements Closeable {

	public static final String TAG = MonitorManager.class.getSimpleName();
	private List<Monitor<?>> monitors = new ArrayList<>();

	public void startMonitor(MonitorType monitorType, Object... args) throws Exception {
		if (null == monitorType) {
			return;
		}
		String clazz = monitorType.getClazz();
		Class<?> monitorClazz = Class.forName(clazz);
		Class<?>[] argsClass = new Class[args.length];
		for (int i = 0; i < argsClass.length; i++) {
			argsClass[i] = args[i].getClass();
		}
		Object monitor = monitorClazz.getConstructor(argsClass).newInstance(args);
		if (monitor instanceof Monitor) {
			monitors.add((Monitor<?>) monitor);
			((Monitor<?>) monitor).start();
		}
	}

	public void startMonitor(Monitor monitor) throws Exception {
		monitors.add((Monitor<?>) monitor);
		((Monitor<?>) monitor).start();
	}

	@Override
	public void close() throws IOException {
		if (CollectionUtils.isEmpty(monitors)) {
			return;
		}
		for (Monitor<?> monitor : monitors) {
			CommonUtils.ignoreAnyError(monitor::close, TAG);
		}
	}

	public Object get(MonitorType monitorType) {
		assert null != monitorType;
		String clazz = monitorType.getClazz();
		Monitor<?> findMonitor = monitors.stream().filter(monitor -> monitor.getClass().getName().equals(clazz)).findFirst().orElse(null);
		if (null != findMonitor) {
			return findMonitor.get();
		} else {
			return null;
		}
	}

	public Monitor<?> getMonitorByType(MonitorType monitorType) {
		assert null != monitorType;
		String clazz = monitorType.getClazz();
		return monitors.stream().filter(monitor -> monitor.getClass().getName().equals(clazz)).findFirst().orElse(null);
	}

	public enum MonitorType {
		SOURCE_TS_MONITOR("io.tapdata.flow.engine.V2.monitor.impl.SourceTSMonitor"),
		TASK_PING_TIME("io.tapdata.flow.engine.V2.monitor.impl.TaskPingTimeMonitor"),
		TABLE_MONITOR("io.tapdata.flow.engine.V2.monitor.impl.TableMonitor"),
		;

		private final String clazz;

		MonitorType(String clazz) {
			this.clazz = clazz;
		}

		public String getClazz() {
			return clazz;
		}
	}

	@Override
	public String toString() {
		if (CollectionUtils.isEmpty(monitors)) {
			return "No monitor";
		}
		List<String> list = new ArrayList<>();
		for (Monitor<?> monitor : monitors) {
			list.add(monitor.describe());
		}
		return "Monitors: \n  - " + String.join("\n  - ", list);
	}
}
