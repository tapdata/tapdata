package io.tapdata.modules.api.net.message;

import io.tapdata.entity.serializer.JavaCustomSerializer;

public abstract class TapEntityEx implements TapEntity {
	private Throwable parseError;

	public Throwable getParseError() {
		return parseError;
	}

	public void setParseError(Throwable parseError) {
		this.parseError = parseError;
	}
}
