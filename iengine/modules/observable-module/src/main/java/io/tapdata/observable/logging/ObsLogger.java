package io.tapdata.observable.logging;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.observable.logging.tag.LogTag;

import java.util.concurrent.Callable;

/**
 * @author Dexter
 **/
public abstract class ObsLogger {
	abstract MonitoringLogsDto.MonitoringLogsDtoBuilder logBaseBuilder();
	abstract void debug(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params);
	abstract void info(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params);
	abstract void warn(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params);
	abstract void error(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params);
	abstract void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params);

	// debug level public logger api

	public void debug(String message, Object... params) {
		debug(this::logBaseBuilder, message, params);
	}

	public void debug(LogTag logTag1, String message, Object... params) {
		debug(() -> logBaseBuilderWithLogTag(logTag1), message, params);
	}

	// info level public logger api

	public void info(String message, Object... params) {
		info(this::logBaseBuilder, message, params);
	}

	public void info(LogTag logTag1, String message, Object... params) {
		info(() -> logBaseBuilderWithLogTag(logTag1), message, params);
	}

	public void info(LogTag logTag1, LogTag logTag2, String message, Object... params) {
		info(() -> logBaseBuilderWithLogTag(logTag1, logTag2), message, params);
	}

	// warn level public logger api

	public void warn(String message, Object... params) {
		warn(this::logBaseBuilder, message, params);
	}

	public void warn(LogTag logTag1, String message, Object... params) {
		warn(() -> logBaseBuilderWithLogTag(logTag1), message, params);
	}

	public void warn(LogTag logTag1, LogTag logTag2, String message, Object... params) {
		warn(() -> logBaseBuilderWithLogTag(logTag1, logTag2), message, params);
	}

	// error level public logger api

	public void error(String message, Object... params) {
		error(this::logBaseBuilder, message, params);
	}

	public void error(LogTag logTag1, String message, Object... params) {
		error(() -> logBaseBuilderWithLogTag(logTag1), message, params);
	}

	public void error(LogTag logTag1, LogTag logTag2, String message, Object... params) {
		error(() -> logBaseBuilderWithLogTag(logTag1, logTag2), message, params);
	}

	// fatal level public logger api

	public void fatal(String message, Object... params) {
		fatal(this::logBaseBuilder, message, params);
	}

	public void fatal(LogTag logTag1, String message, Object... params) {
		fatal(() -> logBaseBuilderWithLogTag(logTag1), message, params);
	}

	public void fatal(LogTag logTag1, LogTag logTag2, String message, Object... params) {
		fatal(() -> logBaseBuilderWithLogTag(logTag1, logTag2), message, params);
	}

	MonitoringLogsDto.MonitoringLogsDtoBuilder logBaseBuilderWithLogTag(LogTag... logTags) {
		MonitoringLogsDto.MonitoringLogsDtoBuilder builder = logBaseBuilder();
		for (LogTag logTag : logTags) {
			if (null != logTag) {
				builder.logTag(logTag.getTag());
			}
		}

		return builder;
	}
}
