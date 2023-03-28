package io.tapdata.observable.logging.appender;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import lombok.SneakyThrows;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author jackin
 * @date 2022/6/20 11:55
 **/
public class AppenderFactory implements Serializable {
	public static final String OBS_LOGGER_TAILER_ID = "OBS-LOGGER-TAILER";
	private volatile static AppenderFactory INSTANCE;

	public static AppenderFactory getInstance() {
		if (INSTANCE == null) {
			synchronized (AppenderFactory.class) {
				if (INSTANCE == null) {
					INSTANCE = new AppenderFactory();
				}
			}
		}
		return INSTANCE;
	}

	private final Logger logger = LogManager.getLogger(AppenderFactory.class);
	public final static int BATCH_SIZE = 100;
	private final static int BATCH_INTERVAL = 500;
	private final static String APPEND_LOG_THREAD_NAME = "";
	private final static String CACHE_QUEUE_DIR = "CacheObserveLogs";

	private Long lastFlushAt;
	private ChronicleQueue cacheLogsQueue;

	private final List<Appender<MonitoringLogsDto>> appenders = new ArrayList<>();

	private final Semaphore emptyWaiting = new Semaphore(1);

	private final ExecutorService executorService = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1),
			r -> new Thread(r, APPEND_LOG_THREAD_NAME)
	);

	private AppenderFactory() {

		String cacheLogsDir = "." + File.separator + CACHE_QUEUE_DIR;
		cacheLogsQueue = ChronicleQueue.singleBuilder(cacheLogsDir)
				.rollCycle(RollCycles.HUGE_DAILY)
				.storeFileListener((cycle, file) -> {
					logger.info("Delete chronic released store file: {}, cycle: {}", file, cycle);
					FileUtils.deleteQuietly(file);
				}).build();

		executorService.submit(() -> {
			ExcerptTailer tailer = cacheLogsQueue.createTailer(OBS_LOGGER_TAILER_ID);
			List<MonitoringLogsDto> logsDtos = new ArrayList<>();
			if (null == lastFlushAt) {
				lastFlushAt = System.currentTimeMillis();
			}
			while (true) {
				try {
					final MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();

					boolean success = tailer.readDocument(r -> decodeFromWireIn(r.getValueIn(), builder));
					if (success) {
						logsDtos.add(builder.build());
					} else {
						emptyWaiting.tryAcquire(1, 200, TimeUnit.MILLISECONDS);
					}
					if (logsDtos.size() >= BATCH_SIZE || System.currentTimeMillis() - lastFlushAt >= BATCH_INTERVAL) {
						if (logsDtos.size() == 0) {
							lastFlushAt = System.currentTimeMillis();
							continue;
						}

						for (Appender<MonitoringLogsDto> appender : appenders) {
							if (null == appender) {
								continue;
							}
							appender.append(logsDtos);
						}
						logsDtos.clear();
						lastFlushAt = System.currentTimeMillis();
					}
				} catch (Exception e) {
					logger.warn("failed to append task logs, error: {}", e.getMessage(), e);
				}
			}
		});
	}

	public void register(Appender logAppender) {
		appenders.add(logAppender);
	}

	public void appendLog(MonitoringLogsDto logsDto) {
		try {
			cacheLogsQueue.acquireAppender().writeDocument(w -> {
				final ValueOut valueOut = w.getValueOut();
				final Date date = logsDto.getDate();
				final String dateString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
				valueOut.writeString(dateString);
				valueOut.writeString(logsDto.getLevel());
				valueOut.writeString(logsDto.getErrorStack());
				valueOut.writeString(logsDto.getMessage());
				valueOut.writeString(logsDto.getTaskId());
				valueOut.writeString(logsDto.getTaskRecordId());
				valueOut.writeLong(logsDto.getTimestamp());
				valueOut.writeString(logsDto.getTaskName());
				valueOut.writeString(logsDto.getNodeId());
				valueOut.writeString(logsDto.getNodeName());
				valueOut.writeString(logsDto.getErrorCode());
				valueOut.writeString(logsDto.getFullErrorCode());
				final String logTagsJoinStr = String.join(",", CollectionUtils.isNotEmpty(logsDto.getLogTags()) ? logsDto.getLogTags() : new ArrayList<>(0));
				valueOut.writeString(logTagsJoinStr);
				if (null != logsDto.getData()) {
					valueOut.writeString(JSON.toJSON(logsDto.getData()).toString());
				}
			});
		} catch (InterruptedRuntimeException ignored) {
		}
		if (emptyWaiting.availablePermits() < 1) {
			emptyWaiting.release(1);
		}
	}

	@SneakyThrows
	private void decodeFromWireIn(ValueIn valueIn, MonitoringLogsDto.MonitoringLogsDtoBuilder builder) {
		final String dateString = valueIn.readString();
		final Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(dateString);
		builder.date(date);
		final String level = valueIn.readString();
		builder.level(level);
		final String errorStack = valueIn.readString();
		builder.errorStack(errorStack);
		final String message = valueIn.readString();
		builder.message(message);
		final String taskId = valueIn.readString();
		builder.taskId(taskId);
		final String taskRecordId = valueIn.readString();
		builder.taskRecordId(taskRecordId);
		final long timestamp = valueIn.readLong();
		builder.timestamp(timestamp);
		final String taskName = valueIn.readString();
		builder.taskName(taskName);
		final String nodeId = valueIn.readString();
		builder.nodeId(nodeId);
		final String nodeName = valueIn.readString();
		builder.nodeName(nodeName);
		final String errorCode = valueIn.readString();
		builder.errorCode(errorCode);
		final String fullErrorCode = valueIn.readString();
		builder.fullErrorCode(fullErrorCode);
		final String logTaskStr = valueIn.readString();
		if (StringUtils.isNotBlank(logTaskStr)) {
			builder.logTags(Arrays.asList(logTaskStr.split(",")));
		}
		final String dataStr = valueIn.readString();
		if (StringUtils.isNotBlank(dataStr)) {
			try {
				builder.data((Collection<? extends Map<String, Object>>) JSON.parseArray(dataStr, (new HashMap<String, Object>()).getClass()));
			} catch (Exception e) {
				logger.error("Read log from file cache queue failed, parse dataStr json failed: {}", dataStr, e);
			}

		}
	}

	private <T> T nullStringProcess(String inputString, Supplier<T> nullSupplier, Supplier<T> getResult) {
		return "null".equals(inputString) || null == inputString ? nullSupplier.get() : getResult.get();
	}
}
