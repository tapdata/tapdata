package com.tapdata.processor.dataflow.aggregation.incr.task;

import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.aggregation.incr.service.AggregationService;
import com.tapdata.processor.dataflow.aggregation.incr.service.LifeCycleService;
import com.tapdata.processor.dataflow.aggregation.incr.service.SyncVersionService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CleanScheduler implements LifeCycleService, Runnable {

	private static final Logger log = LogManager.getLogger(CleanScheduler.class);

	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
	private final AggregationService aggregationService;
	private final SyncVersionService syncVersionService;
	private final long seconds;

	public CleanScheduler(Stage stage, AggregationService aggregationService, SyncVersionService syncVersionService) {
		this.aggregationService = aggregationService;
		this.syncVersionService = syncVersionService;
		this.seconds = stage.getAggrCleanSecond();
	}

	@Override
	public void start() {
		scheduledExecutorService.schedule(this, seconds, TimeUnit.SECONDS);
	}

	@Override
	public void destroy() {
		scheduledExecutorService.shutdownNow();
	}

	@Override
	public void run() {
		// 1. 查找当前 stage 最新版本号
		// 2. 通过 AggregationService 删除版本号小于 最新版本号 的记录
		final long currentVersion = syncVersionService.currentVersion();
		long start = System.nanoTime();
		try {
			log.info("star clean expire version less than {}", currentVersion);
			log.info("end  clean expire version less than {}, record count: {}, cost: {} s", currentVersion, aggregationService.removeExpire(currentVersion), TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start));
		} catch (Throwable t) {
			log.error(String.format("end clean expire version less than %d, cost: %d s, exception: %s", currentVersion, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start), t.getMessage()), t);
			throw t;
		} finally {
			scheduledExecutorService.schedule(this, seconds, TimeUnit.SECONDS);
		}
	}

}
