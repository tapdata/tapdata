package io.tapdata.flow.engine.V2.log;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.FormatUtils;

import java.util.LinkedList;
import java.util.List;

public class CollectLog implements Log {

	public static class Log {
		private String level;
		private String message;

		private Long timestamp;

		public Log(String level, String message, Long timestamp) {
			this.level = level;
			this.message = message;
			this.timestamp = timestamp;
		}

		public String getLevel() {
			return level;
		}

		public void setLevel(String level) {
			this.level = level;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		public Long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(Long timestamp) {
			this.timestamp = timestamp;
		}
	}

	private final List<CollectLog.Log> logs = new LinkedList<>();

	public List<Log> getLogs() {
		return logs;
	}

	@Override
	public void debug(String message, Object... params) {
		logs.add(new Log("DEBUG", FormatUtils.format(message, params), System.currentTimeMillis()));
	}

	@Override
	public void info(String message, Object... params) {
		logs.add(new Log("INFO", FormatUtils.format(message, params), System.currentTimeMillis()));
	}

	@Override
	public void warn(String message, Object... params) {
		logs.add(new Log("WARN", FormatUtils.format(message, params), System.currentTimeMillis()));
	}

	@Override
	public void error(String message, Object... params) {
		logs.add(new Log("ERROR", FormatUtils.format(message, params), System.currentTimeMillis()));
	}

	@Override
	public void error(String message, Throwable throwable) {
		logs.add(new Log("ERROR", FormatUtils.format(message, throwable), System.currentTimeMillis()));
	}

	@Override
	public void fatal(String message, Object... params) {
		logs.add(new Log("FATAL", FormatUtils.format(message, params), System.currentTimeMillis()));
	}
}
