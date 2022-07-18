package com.tapdata.constant;

import com.tapdata.entity.InitialStat;

import java.util.List;

/**
 * @author jackin
 * @date 2020/9/23 10:23 AM
 **/
public class TapdataShareContext {

	private Object processedOffset;
	private List<InitialStat> initialStats;

	public TapdataShareContext() {
	}

	public Object getProcessedOffset() {
		return processedOffset;
	}

	public void setProcessedOffset(Object processedOffset) {
		this.processedOffset = processedOffset;
	}

	public List<InitialStat> getInitialStats() {
		return initialStats;
	}

	public void setInitialStats(List<InitialStat> initialStats) {
		this.initialStats = initialStats;
	}
}
