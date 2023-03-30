package io.tapdata.observable.logging.appender;

import java.util.List;

public interface Appender<T> {
	default void append(T log) {
		throw new UnsupportedOperationException();
	}

	void append(List<T> logs);

	default void start() {
	}

	default void stop() {
	}
}
