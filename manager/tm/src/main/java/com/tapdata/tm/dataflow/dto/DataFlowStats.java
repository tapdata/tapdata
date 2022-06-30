/**
 * @title: DataFlowStats
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

import java.util.List;
import java.util.Map;

public class DataFlowStats {

	public RuntimeThroughput input;

	public RuntimeThroughput output;

	public RuntimeThroughput insert;

	public RuntimeThroughput update;

	public RuntimeThroughput delete;

	public Long transmissionTime;

	public Long transTimeAvg;

	public Long replicationLag;

	private List<StageRuntimeStats> stagesMetrics;

	private List<Map<String, Object>> totalCount;

	private Overview overview;

	private List<DataFlowProgressDetail> progressGroupByDB;

	public RuntimeThroughput getInput() {
		return input;
	}

	public RuntimeThroughput getOutput() {
		return output;
	}

	public RuntimeThroughput getInsert() {
		return insert;
	}

	public RuntimeThroughput getUpdate() {
		return update;
	}

	public RuntimeThroughput getDelete() {
		return delete;
	}

	public Long getTransmissionTime() {
		return transmissionTime;
	}

	public Long getTransTimeAvg() {
		return transTimeAvg;
	}

	public Long getReplicationLag() {
		return replicationLag;
	}

	public List<StageRuntimeStats> getStagesMetrics() {
		return stagesMetrics;
	}

	public List<Map<String, Object>> getTotalCount() {
		return totalCount;
	}

	public Overview getOverview() {
		return overview;
	}

	public List<DataFlowProgressDetail> getProgressGroupByDB() {
		return progressGroupByDB;
	}

	public void setInput(RuntimeThroughput input) {
		this.input = input;
	}

	public void setOutput(RuntimeThroughput output) {
		this.output = output;
	}

	public void setInsert(RuntimeThroughput insert) {
		this.insert = insert;
	}

	public void setUpdate(RuntimeThroughput update) {
		this.update = update;
	}

	public void setDelete(RuntimeThroughput delete) {
		this.delete = delete;
	}

	public void setTransmissionTime(Long transmissionTime) {
		this.transmissionTime = transmissionTime;
	}

	public void setTransTimeAvg(Long transTimeAvg) {
		this.transTimeAvg = transTimeAvg;
	}

	public void setReplicationLag(Long replicationLag) {
		this.replicationLag = replicationLag;
	}

	public void setStagesMetrics(List<StageRuntimeStats> stagesMetrics) {
		this.stagesMetrics = stagesMetrics;
	}

	public void setTotalCount(List<Map<String, Object>> totalCount) {
		this.totalCount = totalCount;
	}

	public void setOverview(Overview overview) {
		this.overview = overview;
	}

	public void setProgressGroupByDB(List<DataFlowProgressDetail> progressGroupByDB) {
		this.progressGroupByDB = progressGroupByDB;
	}
}
