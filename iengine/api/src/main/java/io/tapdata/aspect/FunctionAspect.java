package io.tapdata.aspect;

public abstract class FunctionAspect<T extends FunctionAspect<?>> extends DataNodeAspect<T> {
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
}
