package io.tapdata.entity.memory;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Store last data in your module.
 * Every data here need be serialized to json.
 *
 * Need be caution that memory usage for the data should not be too high.
 */
public class LastData {
	private Object data;
	public LastData data(Object data) {
		this.data = data;
		return this;
	}
	private String error;
//	private String errorStackTrace;
	public LastData error(String error) {
		this.error = error;
//		this.errorStackTrace = Objects.requireNonNull(InstanceFactory.instance(TapUtils.class)).getStackTrace(error);
		return this;
	}

	private Long time;
	public LastData time(Long time) {
		this.time = time;
		return this;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public static <T> T traceLastData(Supplier<T> supplier, Consumer<LastData> consumer) {
		if(supplier == null)
			return null;
		if(consumer == null)
			return supplier.get();
		LastData lastData = null;
		try {
			return supplier.get();
		} catch (Throwable throwable) {
			lastData = new LastData().time(System.currentTimeMillis()).error(throwable.getMessage());
			throw throwable;
		} finally {
			consumer.accept(lastData);
		}
	}
}
