package io.tapdata.observable.logging.appender;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.observable.logging.cache.CacheLogSink;
import io.tapdata.observable.logging.cache.MonitoringLogCodec;
import io.tapdata.observable.logging.cache.TaskCacheManager;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.ValueIn;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * @author jackin
 * @date 2022/6/20 11:55
 **/
public class AppenderFactory {
	public static final String FILE_APPENDER_TAILER_ID = "FILE_APPENDER_TAILER";
	public static final String DEBUG_FILE_APPENDER_TAILER_ID = "DEBUG_FILE_APPENDER_TAILER";
	public static final String TM_APPENDER_TAILER_ID = "TM_APPENDER_TAILER";
	private static final String DEBUG_APPENDER_SUFFIX = "_debug";
	private static volatile AppenderFactory INSTANCE;

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
	public static final int BATCH_SIZE = 100;
	/**
	 * Test-only compatibility hook for existing Wire-format tests. Production writes always
	 * use {@link TaskCacheManager} and never initialize a shared queue.
	 */
	private SingleChronicleQueue cacheLogsQueue;
	private final TaskCacheManager taskCacheManager;
	private final MonitoringLogCodec codec = new MonitoringLogCodec();
	private final Map<String, List<Appender<MonitoringLogsDto>>> appenderMap = new ConcurrentHashMap<>();
	private final Semaphore emptyWaiting = new Semaphore(1);
	private long cycle;

	private AppenderFactory() {
		taskCacheManager = TaskCacheManager.createDefault((log, sink) ->
				appenderAppendLog(log, sink == CacheLogSink.FILE
						? FILE_APPENDER_TAILER_ID
						: TM_APPENDER_TAILER_ID));
		Runtime.getRuntime().addShutdownHook(new Thread(this::closeAll, "CacheObserveLogs-Shutdown"));
	}

	protected void readMessageFromCacheQueue(ExcerptTailer tailer,String tailerType) {
		try {
			final MonitoringLogsDto.MonitoringLogsDtoBuilder builder = MonitoringLogsDto.builder();
			boolean success = tailer.readDocument(r -> decodeFromWireIn(r.getValueIn(), builder));
			if (success) {
				appenderAppendLog(builder,tailerType);
			} else {
				emptyWaiting.tryAcquire(1, 200, TimeUnit.MILLISECONDS);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			logger.warn("Failed to append task logs", e);
		}
	}

	protected void appenderAppendLog(MonitoringLogsDto.MonitoringLogsDtoBuilder builder, String tailerType) {
		appenderAppendLog(builder.build(), tailerType);
	}

	protected void appenderAppendLog(MonitoringLogsDto monitoringLogsDto, String tailerType) {
		String taskId = monitoringLogsDto.getTaskId();
		if (StringUtils.isBlank(taskId)) {
			return;
		}
		List<Appender<MonitoringLogsDto>> appenders = Stream.of(taskId, taskId + DEBUG_APPENDER_SUFFIX)
				.map(appenderMap::get)
				.filter(Objects::nonNull)
				.flatMap(Collection::stream)
				.toList();
		appenders.stream()
				.filter(Objects::nonNull)
				.filter(appender -> supportsSink(appender, tailerType))
				.forEach(appender -> appender.append(monitoringLogsDto));
	}

	private static boolean supportsSink(Appender<MonitoringLogsDto> appender, String tailerType) {
		if (FILE_APPENDER_TAILER_ID.equals(tailerType)) {
			return appender instanceof FileAppender;
		}
		return appender instanceof ObsHttpTMAppender || appender instanceof ScriptNodeProcessNodeAppender;
	}

	protected void deleteFileIfLessThanCurrentCycle(int cycle, File file) {
		if (cycle < this.cycle) {
			boolean successFlag = FileUtils.deleteQuietly(file);
			logger.info("Delete chronic released store file: {}, success: {}. cycle: {}", file, successFlag, cycle);
		}
		this.cycle = cycle;
	}

	public void addTaskAppender(BaseTaskAppender<MonitoringLogsDto> taskAppender) {
		if (null == taskAppender) {
			return;
		}
		String taskId = taskAppender.getTaskId();
		if (StringUtils.isBlank(taskId)) {
			return;
		}
		addAppender(taskId, taskAppender);
	}

	public void addAppender(String key, Appender<MonitoringLogsDto> appender) {
		if (StringUtils.isBlank(key) || appender == null) {
			return;
		}
		this.appenderMap.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(appender);
	}

	public void activateTask(String taskId, String taskName) {
		taskCacheManager.activateTask(taskId, taskName);
	}

	public void deactivateTask(String taskId) {
		taskCacheManager.deactivateTask(taskId);
	}

	public void removeAppenders(String key) {
		taskCacheManager.deactivateTask(key);
		this.appenderMap.remove(key);
		this.appenderMap.remove(key + DEBUG_APPENDER_SUFFIX);
	}

	public void appendLog(MonitoringLogsDto logsDto) {
		if (cacheLogsQueue == null) {
			taskCacheManager.append(logsDto);
			return;
		}
		try (ExcerptAppender excerptAppender = cacheLogsQueue.acquireAppender()) {
			excerptAppender.writeDocument(w -> codec.write(w.getValueOut(), logsDto));
		} catch (InterruptedRuntimeException e) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			logger.warn("Append log in cache queue failed", e);
		}
		if (emptyWaiting.availablePermits() < 1) {
			emptyWaiting.release(1);
		}
	}

	public void appendLogWithoutCache(MonitoringLogsDto logsDto) {
		appendLogWithoutCache(logsDto, FILE_APPENDER_TAILER_ID);
		appendLogWithoutCache(logsDto, TM_APPENDER_TAILER_ID);
	}

	private void appendLogWithoutCache(MonitoringLogsDto logsDto, String tailerType) {
		try {
			appenderAppendLog(logsDto, tailerType);
		} catch (RuntimeException e) {
			logger.warn("Failed to append test task log directly, taskId: {}, tailerType: {}, error: {}",
					logsDto.getTaskId(), tailerType, e.getMessage(), e);
		}
	}

	public void deleteTaskCache(String taskId) {
		this.appenderMap.remove(taskId);
		this.appenderMap.remove(taskId + DEBUG_APPENDER_SUFFIX);
		taskCacheManager.deleteTaskCache(taskId);
	}

	public void closeAll() {
		taskCacheManager.close();
		if (cacheLogsQueue != null) {
			cacheLogsQueue.close();
		}
	}

	protected void decodeFromWireIn(ValueIn valueIn, MonitoringLogsDto.MonitoringLogsDtoBuilder builder) {
		codec.read(valueIn, builder);
	}
}
