package io.tapdata.aspect;

public class SourceCDCDelayAspect extends DataNodeAspect<SourceCDCDelayAspect> {

	private long delay;
	public SourceCDCDelayAspect delay(long delay) {
		this.delay = delay;
		return this;
	}

	public long getDelay() {
		return delay;
	}

	public void setDelay(long delay) {
		this.delay = delay;
	}
}
