/**
 * @title: GranularityInfo
 * @description:
 * @author lk
 * @date 2021/9/15
 */
package com.tapdata.tm.ws.dto;

import com.google.gson.annotations.SerializedName;
import com.tapdata.manager.common.utils.StringUtils;

public class GranularityInfo {

	@SerializedName("data_overview")
	private String dataOverview;

	@SerializedName("repl_lag")
	private String replLag;

	private String throughput;

	@SerializedName("trans_time")
	private String transTime;

	public String getDataOverview() {
		return StringUtils.isBlank(dataOverview) ? "second" : dataOverview;
	}

	public String getReplLag() {
		return StringUtils.isBlank(replLag) ? "second" : replLag;
	}

	public String getThroughput() {
		return StringUtils.isBlank(throughput) ? "second" : throughput;
	}

	public String getTransTime() {
		return StringUtils.isBlank(transTime) ? "second" : transTime;
	}

	public void setDataOverview(String dataOverview) {
		this.dataOverview = dataOverview;
	}

	public void setReplLag(String replLag) {
		this.replLag = replLag;
	}

	public void setThroughput(String throughput) {
		this.throughput = throughput;
	}

	public void setTransTime(String transTime) {
		this.transTime = transTime;
	}
}
