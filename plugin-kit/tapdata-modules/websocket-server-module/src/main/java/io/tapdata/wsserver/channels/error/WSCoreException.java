package io.tapdata.wsserver.channels.error;

import io.tapdata.entity.error.CoreException;

public class WSCoreException extends CoreException {
	private String forId;
	public WSCoreException forId(String forId) {
		this.forId = forId;
		return this;
	}

	public String getForId() {
		return forId;
	}

	public void setForId(String forId) {
		this.forId = forId;
	}
}
