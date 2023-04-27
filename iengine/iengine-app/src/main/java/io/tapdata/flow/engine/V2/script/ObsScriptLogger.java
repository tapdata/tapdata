package io.tapdata.flow.engine.V2.script;

import io.tapdata.entity.logger.Log;
import io.tapdata.observable.logging.ObsLogger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ObsScriptLogger implements Log {

	private final ObsLogger obsLogger;

	private Logger logger;

	public ObsScriptLogger(ObsLogger obsLogger, Logger logger) {
		this.obsLogger = obsLogger;
		this.logger = logger;
	}

	public ObsScriptLogger(ObsLogger obsLogger) {
		this.obsLogger = obsLogger;
		this.logger = LogManager.getLogger(ObsScriptLogger.class);
	}

	@Override
	public void debug(String message, Object... params) {
		obsLogger.debug(message, params);
	}

	@Override
	public void info(CharSequence message) {
		obsLogger.info(String.valueOf(message));
	}

	@Override
	public void info(String message, Object... params) {
		obsLogger.info(message, params);
	}

	@Override
	public void warn(String message, Object... params) {
		obsLogger.warn(message, params);
	}

	@Override
	public void error(String message, Object... params) {
		obsLogger.error(message, params);
	}

	@Override
	public void error(String message, Throwable throwable) {
		obsLogger.error(message, throwable);
	}

	@Override
	public void fatal(String message, Object... params) {
		obsLogger.error(message, params);
	}
}
