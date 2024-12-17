package io.tapdata.observable.logging;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;

import java.util.concurrent.Callable;

/**
 * @author samuel
 * @Description
 * @create 2024-10-24 18:58
 **/
public class BlankObsLogger extends ObsLogger {
	@Override
	public MonitoringLogsDto.MonitoringLogsDtoBuilder logBaseBuilder() {
		return null;
	}

	@Override
	public void trace(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {

	}

	@Override
	public void debug(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {

	}

	@Override
	public void info(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {

	}

	@Override
	public void warn(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {

	}

	@Override
	public void error(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params) {

	}

	@Override
	public void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params) {

	}

	@Override
	public boolean isEnabled(LogLevel logLevel) {
		return false;
	}

	@Override
	public boolean isInfoEnabled() {
		return false;
	}

	@Override
	public boolean isWarnEnabled() {
		return false;
	}

	@Override
	public boolean isErrorEnabled() {
		return false;
	}

	@Override
	public boolean isDebugEnabled() {
		return false;
	}

	@Override
	public boolean isFatalEnabled() {
		return false;
	}
}
