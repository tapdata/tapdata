package io.tapdata.modules.api.net.message;

import io.tapdata.entity.tracker.MessageTracker;

public abstract class TapEntityEx extends MessageTracker implements TapEntity {
	private Throwable parseError;

	public Throwable getParseError() {
		return parseError;
	}

	public void setParseError(Throwable parseError) {
		this.parseError = parseError;
	}
}
