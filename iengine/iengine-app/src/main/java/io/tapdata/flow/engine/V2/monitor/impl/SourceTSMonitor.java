package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.constant.ExecutorUtil;
import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.task.dto.TaskDto;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description Get the last log time monitor of the source database
 * @create 2022-03-02 01:30
 **/
public class SourceTSMonitor extends TaskMonitor<Long> {

	private static final long INTERVAL_TS = 5000L;

	private Connections connections;
	private ScheduledExecutorService monitorThreadPool;
	private long dbTimestamp;

	public SourceTSMonitor(TaskDto taskDto, Connections connections) {
		super(taskDto);
		assert null != connections;
		this.connections = connections;
	}

	@Override
	public void start() {
		this.monitorThreadPool = new ScheduledThreadPoolExecutor(1, r -> {
			Thread thread = new Thread(r);
			thread.setName("Source-TS-Monitor-" + taskDto.getName() + "-" + taskDto.getId()
					+ "-" + connections.getName() + "-" + connections.getId());
			return thread;
		});

		this.monitorThreadPool.scheduleAtFixedRate(() -> {
//      try {
//        String timezone = TimeZoneUtil.getZoneIdByDatabaseType(connections);
//        String dateString = TimeZoneUtil.getDateByDatabaseType(connections, timezone);
//        String pattern = DateUtil.determineDateFormat(dateString);
//        assert pattern != null;
//        LocalDateTime localDateTime = LocalDateTime.parse(dateString, DateTimeFormatter.ofPattern(pattern));
//        Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
//        this.dbTimestamp = instant.toEpochMilli();
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
		}, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
	}

	@Override
	public Long get() {
		return dbTimestamp;
	}

	@Override
	public void close() throws IOException {
		ExecutorUtil.shutdown(monitorThreadPool, 5L, TimeUnit.SECONDS);
	}
}
