package io.tapdata.observable.logging.appender;

import java.util.List;

public interface Appender<T> {

	void append(List<T> logs);
}
