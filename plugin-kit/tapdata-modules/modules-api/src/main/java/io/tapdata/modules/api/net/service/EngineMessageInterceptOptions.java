package io.tapdata.modules.api.net.service;

import io.tapdata.pdk.apis.entity.message.EngineMessage;

public class EngineMessageInterceptOptions {
	private boolean intercepted;
	public EngineMessageInterceptOptions intercepted(boolean intercepted) {
		this.intercepted = intercepted;
		return this;
	}
	private String reason;
	public EngineMessageInterceptOptions reason(String reason) {
		this.reason = reason;
		return this;
	}

	public boolean isIntercepted() {
		return intercepted;
	}

	public void setIntercepted(boolean intercepted) {
		this.intercepted = intercepted;
	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}
}
