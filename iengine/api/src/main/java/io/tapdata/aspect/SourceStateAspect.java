package io.tapdata.aspect;

public class SourceStateAspect extends DataNodeAspect<SourceStateAspect> {
	public final static int STATE_INITIAL_SYNC_START = 5;
	public final static int STATE_INITIAL_SYNC_COMPLETED = 8;

	public final static int STATE_CDC_START = 10;
	public final static int STATE_CDC_COMPLETED = 15;

	private Long initialSyncStartTime;
	public SourceStateAspect initialSyncStartTime(long initialSyncStartTime) {
		this.initialSyncStartTime = initialSyncStartTime;
		return this;
	}

	private Long initialSyncCompletedTime;
	public SourceStateAspect initialSyncCompletedTime(long initialSyncCompletedTime) {
		this.initialSyncCompletedTime = initialSyncCompletedTime;
		return this;
	}

	private Long cdcStartTime;
	public SourceStateAspect cdcStartTime(long cdcStartTime) {
		this.cdcStartTime = cdcStartTime;
		return this;
	}

	private Long cdcCompletedTime;
	public SourceStateAspect cdcCompletedTime(long cdcCompletedTime) {
		this.cdcCompletedTime = cdcCompletedTime;
		return this;
	}

	private int state;
	public synchronized SourceStateAspect state(int state) {
		switch (state) {
			case STATE_CDC_START:
				cdcStartTime = System.currentTimeMillis();
				break;
			case STATE_CDC_COMPLETED:
				cdcCompletedTime = System.currentTimeMillis();
				break;
			case STATE_INITIAL_SYNC_START:
				initialSyncStartTime = System.currentTimeMillis();
				break;
			case STATE_INITIAL_SYNC_COMPLETED:
				initialSyncCompletedTime = System.currentTimeMillis();
				break;
		}

		if(this.state != state) {
			this.state = state;
		}
		return this;
	}

	public Long getInitialSyncStartTime() {
		return initialSyncStartTime;
	}

	public void setInitialSyncStartTime(Long initialSyncStartTime) {
		this.initialSyncStartTime = initialSyncStartTime;
	}

	public Long getInitialSyncCompletedTime() {
		return initialSyncCompletedTime;
	}

	public void setInitialSyncCompletedTime(Long initialSyncCompletedTime) {
		this.initialSyncCompletedTime = initialSyncCompletedTime;
	}

	public Long getCdcStartTime() {
		return cdcStartTime;
	}

	public void setCdcStartTime(Long cdcStartTime) {
		this.cdcStartTime = cdcStartTime;
	}

	public Long getCdcCompletedTime() {
		return cdcCompletedTime;
	}

	public void setCdcCompletedTime(Long cdcCompletedTime) {
		this.cdcCompletedTime = cdcCompletedTime;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}
}
