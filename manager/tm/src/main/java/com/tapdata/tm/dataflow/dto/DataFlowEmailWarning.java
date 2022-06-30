/**
 * @title: DataFlowEmailWarning
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class DataFlowEmailWarning {

	private Boolean edited;

	private Boolean started;

	private Boolean error;

	private Boolean paused;

	public Boolean getEdited() {
		return edited;
	}

	public Boolean getStarted() {
		return started;
	}

	public Boolean getError() {
		return error;
	}

	public Boolean getPaused() {
		return paused;
	}

	public void setEdited(Boolean edited) {
		this.edited = edited;
	}

	public void setStarted(Boolean started) {
		this.started = started;
	}

	public void setError(Boolean error) {
		this.error = error;
	}

	public void setPaused(Boolean paused) {
		this.paused = paused;
	}
}
