package io.tapdata.flow.engine.V2.common;

import com.hazelcast.persistence.store.StoreLogger;
import io.tapdata.observable.logging.ObsLogger;

/**
 * @author samuel
 * @Description
 * @create 2023-11-08 15:43
 **/
public class StoreLoggerImpl implements StoreLogger {
	private ObsLogger obsLogger;

	public StoreLoggerImpl(ObsLogger obsLogger) {
		this.obsLogger = obsLogger;
	}

	@Override
	public void info(String message, Object... args) {
		obsLogger.info(message, args);
	}

	@Override
	public void warn(String message, Object... args) {
		obsLogger.warn(message, args);
	}

	@Override
	public void error(String message, Object... args) {
		obsLogger.error(message, args);
	}

	@Override
	public void debug(String message, Object... args) {
		obsLogger.debug(message, args);
	}

	@Override
	public boolean isDebugEnabled() {
		return obsLogger.isDebugEnabled();
	}
}
