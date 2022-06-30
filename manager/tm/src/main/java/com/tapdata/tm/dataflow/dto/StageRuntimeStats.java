/**
 * @title: StageRuntimeStats
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class StageRuntimeStats {

	private String stageId;

	public RuntimeThroughput input;

	public RuntimeThroughput output;

	public RuntimeThroughput insert;

	public RuntimeThroughput update;

	public RuntimeThroughput delete;

	public Long transmissionTime;

	public Long transTimeAvg;

	public Long replicationLag;

	private String status;

	public String getStageId() {
		return stageId;
	}

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

	public String getStatus() {
		return status;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
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

	public void setStatus(String status) {
		this.status = status;
	}
}
