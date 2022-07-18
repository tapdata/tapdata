package io.tapdata.common;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.filter.AbstractFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Plugin(name = "TapdataLog4jFilter",
		category = Node.CATEGORY,
		elementType = Filter.ELEMENT_TYPE, printObject = true)
public class TapdataLog4jFilter extends AbstractFilter {

	private static Map<String, List<TapdataLogEvent>> messages = new WeakHashMap<>();
	private final Level level;
	private final int interval;
	private final boolean isFormat;

	private final static ScheduledExecutorService clearMessagesExecutors = Executors.newScheduledThreadPool(1);

	private TapdataLog4jFilter(Level level, int interval, boolean isFormat, Result onmatch, Result onmismatch) {
		super(onmatch, onmismatch);
		this.level = level;
		this.interval = interval;
		this.isFormat = isFormat;

		startClearScheduledThread();
	}

	@Override
	public Result filter(LogEvent logEvent) {
		Level level = logEvent.getLevel();
		String format = logEvent.getMessage() == null ? "" : logEvent.getMessage().getFormat();
		String formattedMessage = logEvent.getMessage() == null ? "" : logEvent.getMessage().getFormattedMessage();
		String message = isFormat ? formattedMessage : format;
		String jobId = logEvent.getContextData() == null ? "" : logEvent.getContextData().getValue("jobId");
		long now = logEvent.getTimeMillis();

		try {
			if (StringUtils.isNotBlank(jobId)) {
				if (!this.level.isLessSpecificThan(level)) {
					return this.onMismatch;
				} else {
					if (messages.containsKey(jobId)) {
						List<TapdataLogEvent> logs = messages.get(jobId);
						boolean isFind = false;
						boolean isMatch = false;
						for (TapdataLogEvent event : logs) {
							long time = event.getTime();
							if (event.getMessage().equals(message)) {
								isFind = true;
								if (now / 1000 - time / 1000 <= interval) {
									isMatch = true;
								} else {
									event.setTime(now);
								}
								break;
							}
						}
						if (!isFind) {
							logs.add(new TapdataLogEvent(now, message));
						}

						return isMatch ? this.onMatch : this.onMismatch;
					} else {
						List<TapdataLogEvent> logs = new ArrayList<>();
						logs.add(new TapdataLogEvent(now, message));
						messages.put(jobId, logs);
					}
				}
			}
		} catch (Exception e) {
			return this.onMismatch;
		}

		return Result.NEUTRAL;
	}

	@Override
	public String toString() {
		return "TapdataLog4jFilter{" +
				"level=" + level +
				", interval=" + interval +
				", isFormat=" + isFormat +
				'}';
	}

	public static class Builder implements org.apache.logging.log4j.core.util.Builder<TapdataLog4jFilter> {
		@PluginBuilderAttribute
		private Level level;
		@PluginBuilderAttribute
		private int interval; // seconds
		@PluginBuilderAttribute
		private boolean isFormat;
		@PluginBuilderAttribute
		private Result onmatch;
		@PluginBuilderAttribute
		private Result onmismatch;

		public Builder() {
			this.level = Level.ALL;
			this.interval = 20;
			this.onmatch = Result.DENY;
			this.onmismatch = Result.NEUTRAL;
			this.isFormat = true;
		}

		public TapdataLog4jFilter.Builder setLevel(Level level) {
			this.level = level;
			return this;
		}

		public TapdataLog4jFilter.Builder setInterval(int interval) {
			this.interval = interval;
			return this;
		}

		public TapdataLog4jFilter.Builder setOnmatch(Result onmatch) {
			this.onmatch = onmatch;
			return this;
		}

		public TapdataLog4jFilter.Builder setOnmismatch(Result onmismatch) {
			this.onmismatch = onmismatch;
			return this;
		}

		public TapdataLog4jFilter.Builder setFormat(boolean format) {
			isFormat = format;
			return this;
		}

		@Override
		public TapdataLog4jFilter build() {
			return new TapdataLog4jFilter(level, interval, isFormat, onmatch, onmismatch);
		}
	}

	private static class TapdataLogEvent {
		private long time;

		private String message;

		public TapdataLogEvent(long time, String message) {
			this.time = time;
			this.message = message;
		}

		public long getTime() {
			return time;
		}

		public TapdataLogEvent setTime(long time) {
			this.time = time;
			return this;
		}

		public String getMessage() {
			return message;
		}

		public TapdataLogEvent setMessage(String message) {
			this.message = message;
			return this;
		}
	}

	private void startClearScheduledThread() {
		clearMessagesExecutors.scheduleAtFixedRate(() -> {
			if (MapUtils.isNotEmpty(messages)) {
				try {
					clearMessages();
				} catch (Exception e) {
					// do nothing
				}
			}
		}, 1, 1, TimeUnit.MINUTES);
	}

	private synchronized void clearMessages() {
		long nowTs = System.currentTimeMillis();
		for (Map.Entry<String, List<TapdataLogEvent>> entry : messages.entrySet()) {
			try {
				List<TapdataLogEvent> logs = entry.getValue();
				if (CollectionUtils.isNotEmpty(logs)) {
					for (int i = 0; i < logs.size(); i++) {
						TapdataLogEvent tapdataLogEvent = logs.get(i);
						if (nowTs - tapdataLogEvent.getTime() > interval) {
							logs.remove(i);
							i--;
						}
					}
				}
			} catch (Exception e) {
				continue;
			}
		}
	}
}
