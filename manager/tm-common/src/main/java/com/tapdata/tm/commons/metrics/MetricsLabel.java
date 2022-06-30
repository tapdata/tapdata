package com.tapdata.tm.commons.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-12-07 19:34
 **/
@EqualsAndHashCode
@Data
public class MetricsLabel implements Serializable {

	private static final long serialVersionUID = 4350410674474007547L;

	protected String app;
	protected String uid;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	protected String customLabel;

	public static enum MetricsLabelAppType {
		FLOW_ENGINE("Flow Engine"),
		;

		private static Map<String, MetricsLabelAppType> appMap;

		static {
			appMap = new HashMap<>();
			for (MetricsLabelAppType value : MetricsLabelAppType.values()) {
				appMap.put(value.type, value);
			}
		}

		private String type;

		MetricsLabelAppType(String type) {
			this.type = type;
		}
	}
}
