package io.tapdata.observable.logging;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.ErrorCodeConfig;
import io.tapdata.ErrorCodeEntity;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.logging.tag.LogTag;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.util.concurrent.Callable;

/**
 * @author Dexter
 **/
public abstract class ObsLogger {
	public abstract MonitoringLogsDto.MonitoringLogsDtoBuilder logBaseBuilder();

	public abstract void debug(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params);

	public abstract void info(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params);

	public abstract void warn(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params);

	public abstract void error(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params);

	public abstract void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable, String message, Object... params);

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

	public void error(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable) {
		error(callable, throwable, null);
	}

	public void error(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		error(callable, null, message, params);
	}

	public void error(String message, Object... params) {
		error(this::logBaseBuilder, null, message, params);
	}

	public void error(String message, Throwable throwable) {
		error(this::logBaseBuilder, throwable, message);
	}

	public void error(LogTag logTag1, String message, Object... params) {
		error(() -> logBaseBuilderWithLogTag(logTag1), null, message, params);
	}

	public void error(LogTag logTag1, LogTag logTag2, String message, Object... params) {
		error(() -> logBaseBuilderWithLogTag(logTag1, logTag2), null, message, params);
	}

	public void error(Throwable throwable, Object... params) {
		error(this::logBaseBuilder, throwable, throwable.getMessage(), params);
	}

	public void error(LogTag logTag1, Throwable throwable, Object... params) {
		error(() -> logBaseBuilderWithLogTag(logTag1), throwable, throwable.getMessage(), params);
	}

	public void error(LogTag logTag1, LogTag logTag2, Throwable throwable, Object... params) {
		error(() -> logBaseBuilderWithLogTag(logTag1, logTag2), throwable, throwable.getMessage(), params);
	}

	// fatal level public logger api

	public void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, Throwable throwable) {
		fatal(callable, throwable, null);
	}

	public void fatal(Callable<MonitoringLogsDto.MonitoringLogsDtoBuilder> callable, String message, Object... params) {
		fatal(callable, null, message, params);
	}

	public void fatal(String message, Object... params) {
		fatal(this::logBaseBuilder, null, message, params);
	}

	public void fatal(LogTag logTag1, String message, Object... params) {
		fatal(() -> logBaseBuilderWithLogTag(logTag1), null, message, params);
	}

	public void fatal(LogTag logTag1, LogTag logTag2, String message, Object... params) {
		fatal(() -> logBaseBuilderWithLogTag(logTag1, logTag2), null, message, params);
	}

	public void fatal(Throwable throwable, Object... params) {
		fatal(this::logBaseBuilder, throwable, throwable.getMessage(), params);
	}

	public void fatal(LogTag logTag1, Throwable throwable, Object... params) {
		fatal(() -> logBaseBuilderWithLogTag(logTag1), throwable, throwable.getMessage(), params);
	}

	public void fatal(LogTag logTag1, LogTag logTag2, Throwable throwable, Object... params) {
		fatal(() -> logBaseBuilderWithLogTag(logTag1, logTag2), throwable, throwable.getMessage(), params);
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

	protected static void buildErrorMessage(
			Throwable throwable,
			ParameterizedMessage parameterizedMessage,
			MonitoringLogsDto.MonitoringLogsDtoBuilder builder
	) {
		String formattedMessage = parameterizedMessage.getFormattedMessage();
		if (StringUtils.isBlank(formattedMessage)) {
			formattedMessage = "<Empty error message>";
		}
		builder.message(parameterizedMessage.getFormattedMessage());
		String stackString = "<-- Full Stack Trace -->\n" + TapSimplify.getStackString(throwable);
		if (throwable instanceof TapCodeException) {
			String errorCode = ((TapCodeException) throwable).getCode();
			builder.errorCode(errorCode);
			ErrorCodeEntity errorCodeEntity = ErrorCodeConfig.getInstance().getErrorCode(errorCode);
			if (null != errorCodeEntity) {
				builder.fullErrorCode(errorCodeEntity.fullErrorCode());
			}
			String simpleStack = ((TapCodeException) throwable).simpleStack();
			if (StringUtils.isNotBlank(simpleStack)) {
				stackString = "<-- Error Message -->\n" + formattedMessage+"\n\n"
						+ "<-- Simple Stack Trace -->\n" + simpleStack + "\n\n"
						+ stackString;
			}
			builder.errorStack(stackString);
		} else {
			builder.errorStack(stackString);
		}
	}
}
