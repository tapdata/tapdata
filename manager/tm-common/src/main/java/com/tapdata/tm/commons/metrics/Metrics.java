package com.tapdata.tm.commons.metrics;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-12-07 17:52
 **/
@EqualsAndHashCode(callSuper = true)
@Data
public class Metrics extends BaseDto implements Serializable {

	private static final long serialVersionUID = -4550377218730470458L;

	private String name;
	private String type;
	private Double value;
	private Long ts;
	private String help;
	private MetricsLabel labels;

	private Metrics() {
	}

	private Metrics(String name, String type, Double value, String help, MetricsLabel labels) {
		this.name = name;
		this.type = type;
		this.value = value;
		this.help = help;
		this.labels = labels;
		this.ts = System.currentTimeMillis();
	}

	public static Metrics counter(String name, Double value, String help, MetricsLabel labels) {
		return new Metrics(
				name,
				MetricsType.COUNTER.type,
				value,
				help,
				labels
		);
	}

	public static Metrics gauge(String name, Double value, String help, MetricsLabel labels) {
		return new Metrics(
				name,
				MetricsType.GAUGE.type,
				value,
				help,
				labels
		);
	}

	public enum MetricsType {
		COUNTER("Counter"),
		GAUGE("Gauge"),
		HISTOGRAM("Histogram"),
		SUMMARY("Summary"),
		;

		private static Map<String, MetricsType> typeMap;

		static {
			typeMap = new HashMap<>();
			for (MetricsType value : MetricsType.values()) {
				typeMap.put(value.type, value);
			}
		}

		public static MetricsType fromType(String type) {
			return typeMap.get(type);
		}

		private String type;

		private MetricsType(String type) {
			this.type = type;
		}
	}
}
