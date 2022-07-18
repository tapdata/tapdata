package io.tapdata.flow.engine.V2.aspect;

public abstract class FunctionAspectData<T extends FunctionAspectData<?>> extends DataNodeAspect<T> {
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
