/**
 * @title: DataFlowInsightCache
 * @description:
 * @author lk
 * @date 2021/9/28
 */
package com.tapdata.tm.ws.dto;

public class DataFlowInsightCache {

	private String sessionId;

	private String dataFlowId;

	private String stageId;

	private GranularityInfo granularityInfo;

	private String statsType;

	public DataFlowInsightCache(String sessionId, MessageInfo messageInfo) {
		this.sessionId = sessionId;
		this.dataFlowId = messageInfo.getDataFlowId();
		this.stageId = messageInfo.getStageId();
		this.granularityInfo = messageInfo.getGranularity();
		this.statsType = messageInfo.getStatsType();
	}

	public String getSessionId() {
		return sessionId;
	}

	public String getDataFlowId() {
		return dataFlowId;
	}

	public GranularityInfo getGranularityInfo() {
		return granularityInfo;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public void setDataFlowId(String dataFlowId) {
		this.dataFlowId = dataFlowId;
	}

	public void setGranularityInfo(GranularityInfo granularityInfo) {
		this.granularityInfo = granularityInfo;
	}

	public String getStageId() {
		return stageId;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
	}

	public String getStatsType() {
		return statsType;
	}

	public void setStatsType(String statsType) {
		this.statsType = statsType;
	}
}
