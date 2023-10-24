package io.tapdata.entity.tracker;

/**
 * @author aplomb
 */
public class MessageTracker {
	protected transient Long createTime;
	public MessageTracker createTime(Long createTime) {
		this.createTime = createTime;
		return this;
	}
	protected transient Integer requestBytes;
	public MessageTracker requestBytes(Integer requestBytes) {
		this.requestBytes = requestBytes;
		return this;
	}
	public MessageTracker requestBytes(byte[] bytes) {
		if(bytes != null) {
			requestBytes = bytes.length;
		} else {
			requestBytes = 0;
		}
		return this;
	}
	protected transient Integer responseBytes;
	public MessageTracker responseBytes(Integer responseBytes) {
		this.responseBytes = responseBytes;
		return this;
	}
	public MessageTracker responseBytes(byte[] bytes) {
		if(bytes != null) {
			responseBytes = bytes.length;
		} else {
			responseBytes = 0;
		}
		return this;
	}
	protected transient Long takes;
	public MessageTracker takes(Long takes) {
		this.takes = takes;
		return this;
	}
	protected transient Throwable throwable;
	public MessageTracker throwable(Throwable throwable) {
		this.throwable = throwable;
		return this;
	}
	protected transient String otherTMIpPort;
	public MessageTracker otherTMIpPort(String otherTMIpPort) {
		this.otherTMIpPort = otherTMIpPort;
		return this;
	}

	protected transient Boolean internalRequest;
	public MessageTracker internalRequest(Boolean internalRequest) {
		this.internalRequest = internalRequest;
		return this;
	}
	public MessageTracker() {
		createTime = System.currentTimeMillis();
	}

	public Long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Long createTime) {
		this.createTime = createTime;
	}

	public Integer getRequestBytes() {
		return requestBytes;
	}

	public void setRequestBytes(Integer requestBytes) {
		this.requestBytes = requestBytes;
	}

	public Integer getResponseBytes() {
		return responseBytes;
	}

	public void setResponseBytes(Integer responseBytes) {
		this.responseBytes = responseBytes;
	}

	public String getOtherTMIpPort() {
		return otherTMIpPort;
	}

	public void setOtherTMIpPort(String otherTMIpPort) {
		this.otherTMIpPort = otherTMIpPort;
	}

	public Long getTakes() {
		return takes;
	}

	public void setTakes(Long takes) {
		this.takes = takes;
	}

	public Throwable getThrowable() {
		return throwable;
	}

	public void setThrowable(Throwable throwable) {
		this.throwable = throwable;
	}

	public Boolean getInternalRequest() {
		return internalRequest;
	}

	public void setInternalRequest(Boolean internalRequest) {
		this.internalRequest = internalRequest;
	}
}
