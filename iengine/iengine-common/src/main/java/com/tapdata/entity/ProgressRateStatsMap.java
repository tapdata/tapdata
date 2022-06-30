package com.tapdata.entity;

import java.util.HashMap;
import java.util.Map;

public class ProgressRateStatsMap {

	private Map<String, ProgressRateStats> statsMap;

	public ProgressRateStatsMap() {
		statsMap = new HashMap<>();
	}

	public Map<String, ProgressRateStats> getStatsMap() {
		return statsMap;
	}

	public void setStatsMap(Map<String, ProgressRateStats> statsMap) {
		this.statsMap = statsMap;
	}
}
