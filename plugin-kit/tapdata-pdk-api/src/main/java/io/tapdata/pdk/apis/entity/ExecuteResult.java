package io.tapdata.pdk.apis.entity;

/**
 * @author aplomb
 */
public class ExecuteResult<T> {
	private T result;
	public ExecuteResult<T> result(T result) {
		this.result = result;
		return this;
	}

	private Throwable error;

	public ExecuteResult<T> error(Throwable error) {
		this.error = error;
		return this;
	}


	public ExecuteResult() {
	}

	public Throwable getError() {
		return error;
	}

	public void setError(Throwable error) {
		this.error = error;
	}

	public T getResult() {
		return result;
	}

	public void setResult(T result) {
		this.result = result;
	}
}
