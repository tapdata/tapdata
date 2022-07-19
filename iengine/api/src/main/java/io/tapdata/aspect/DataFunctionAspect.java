package io.tapdata.aspect;

public abstract class DataFunctionAspect<T extends DataFunctionAspect<?>> extends DataNodeAspect<T> {
	private Long endTime;
	public T endTime(Long endTime) {
		this.endTime = endTime;
		return (T) this;
	}
	public static final int STATE_START = 1;
	public static final int STATE_END = 20;
	private int state;
	public T state(int state) {
		if(this.state != state) {
			this.state = state;
			if(this.state == STATE_END && endTime == null)
				endTime = System.currentTimeMillis();
		}
		return (T) this;
	}
	public T start() {
		this.state = STATE_START;
		return (T) this;
	}
	private Throwable throwable;

	public T throwable(Throwable throwable) {
		this.throwable = throwable;
		return (T) this;
	}

	public Throwable getThrowable() {
		return throwable;
	}

	public void setThrowable(Throwable throwable) {
		this.throwable = throwable;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}
}
