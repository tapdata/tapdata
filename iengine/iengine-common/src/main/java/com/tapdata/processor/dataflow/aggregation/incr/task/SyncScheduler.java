package com.tapdata.processor.dataflow.aggregation.incr.task;

import com.tapdata.constant.TapdataOffset;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.aggregation.incr.cache.BucketCache;
import com.tapdata.processor.dataflow.aggregation.incr.convert.MessageOp;
import com.tapdata.processor.dataflow.aggregation.incr.func.AggrFunction;
import com.tapdata.processor.dataflow.aggregation.incr.service.LifeCycleService;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotService;
import com.tapdata.processor.dataflow.aggregation.incr.service.SyncVersionService;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.AggrBucket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.tapdata.entity.DataQualityTag.SUB_COLUMN_NAME;

public class SyncScheduler implements LifeCycleService, Runnable {

	private static final Logger log = LogManager.getLogger(SyncScheduler.class);

	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
	private final ProcessorContext processorContext;
	private final SnapshotService<?> snapshotService;
	private final SyncVersionService syncVersionService;
	private final List<AggrFunction> aggrFunctionList;
	private final long seconds;
	private final BucketCache bucketCache;
	private final Lock lock;
	private final String table;
	private final Stage processStage;
	private final Stage sourceStage;

	public SyncScheduler(ProcessorContext processorContext, Stage stage, SnapshotService<?> snapshotService, SyncVersionService syncVersionService, List<AggrFunction> aggrFunctionList, BucketCache bucketCache, Lock lock) {
		this.processorContext = processorContext;
		this.processStage = stage;
		this.snapshotService = snapshotService;
		this.syncVersionService = syncVersionService;
		this.aggrFunctionList = aggrFunctionList;
		this.bucketCache = bucketCache;
		this.seconds = stage.getAggrFullSyncSecond();
		this.lock = lock;
		List<Mapping> mappings = this.processorContext.getJob().getMappings();
		this.table = mappings.get(0).getFrom_table();
		List<Stage> stageList = processorContext.getJob().getStages();
		this.sourceStage = stageList.get(0);
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
		long start = System.nanoTime();
		try {
			log.info("start to full sync, try to lock first");
			lock.lock();
			log.info("start to full sync, lock successfully, cost {} s", TimeUnit.NANOSECONDS.toSeconds((System.nanoTime() - start)));
			this.doRun();
		} catch (Throwable t) {
			log.error("full sync exception", t);
		} finally {
			lock.unlock();
			log.info("end to full sync, cost {} s", TimeUnit.NANOSECONDS.toSeconds((System.nanoTime() - start)));
			scheduledExecutorService.schedule(this, seconds, TimeUnit.SECONDS);
		}
	}

	private void doRun() {
		// 1. 全量同步版本号 --> SyncVersionService 获取当前 stage 的同步版本号
		long version = this.syncVersionService.nextVersion();
		for (AggrFunction func : aggrFunctionList) {
			// 2. 通过 SnapshotService 执行分组聚合，组装新的 MessageEntity, 加上 __tapd8 字段, 通过 ProcessContext 传输到下个节点
			List<AggrBucket> bucketList = Optional.ofNullable(func.callByGroup(snapshotService)).orElse(Collections.emptyList());
			List<MessageEntity> messageEntityList = new ArrayList<>(bucketList.size());
			for (AggrBucket<?> bucket : bucketList) {
				final Object msgId;
				final Map<String, Object> dataMap = new HashMap<>();
				if (bucket.getKey().size() > 0) {
					msgId = new LinkedHashMap<>(bucket.getKey());
					((Map) msgId).put("_tapd8_sub_name", func.getProcessName());
					bucket.getKey().forEach(dataMap::put);
				} else {
					msgId = func.getProcessName();
				}
				dataMap.put("_id", msgId);
				dataMap.put(func.getFunc().name(), bucket.getValue());
				dataMap.put(SUB_COLUMN_NAME + ".version", syncVersionService.currentVersion());
				MessageEntity entity = new MessageEntity(MessageOp.INSERT.getType(), dataMap, table);
				entity.setProcessorStageId(this.processStage.getId());
				entity.setSourceStageId(this.sourceStage.getId());
				entity.setOffset(new TapdataOffset(TapdataOffset.SYNC_STAGE_CDC, null));
				messageEntityList.add(entity);
			}
			processorContext.getProcessorHandle().accept(messageEntityList);
		}
		// 3. clean cache
		bucketCache.invalidAll();
	}


}
