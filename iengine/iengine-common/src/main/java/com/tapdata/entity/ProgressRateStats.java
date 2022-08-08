package com.tapdata.entity;

import java.io.Serializable;
import java.util.Map;

/**
 * @author jackin
 */
public class ProgressRateStats implements Serializable {

	public final static String SOURCE_FIELD = "source";
	public final static String TARGET_FIELD = "target";
	public final static String LAG_FIELD = "lag";
	public final static String LAG_PER_FIELD = "lag_percentage";
	private static final long serialVersionUID = -3670308816787697397L;

	private Map<String, Object> row_count;

	private Map<String, Object> ts;

	public ProgressRateStats(Map<String, Object> row_count, Map<String, Object> ts) {
		this.row_count = row_count;
		this.ts = ts;
	}

	public Map<String, Object> getRow_count() {
		return row_count;
	}

	public void setRow_count(Map<String, Object> row_count) {
		this.row_count = row_count;
	}

	public Map<String, Object> getTs() {
		return ts;
	}

	public void setTs(Map<String, Object> ts) {
		this.ts = ts;
	}
}
