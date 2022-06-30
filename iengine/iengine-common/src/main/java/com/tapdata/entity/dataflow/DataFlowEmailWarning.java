package com.tapdata.entity.dataflow;

import java.io.Serializable;

/**
 * @author jackin
 */
public class DataFlowEmailWarning implements Serializable {

	private static final long serialVersionUID = 2141704399065941352L;
	private boolean edited;
	private boolean started;
	private boolean error;
	private boolean paused;

	public boolean getEdited() {
		return edited;
	}

	public void setEdited(boolean edited) {
		this.edited = edited;
	}

	public boolean getStarted() {
		return started;
	}

	public void setStarted(boolean started) {
		this.started = started;
	}

	public boolean getError() {
		return error;
	}

	public void setError(boolean error) {
		this.error = error;
	}

	public boolean getPaused() {
		return paused;
	}

	public void setPaused(boolean paused) {
		this.paused = paused;
	}
}
