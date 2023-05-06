package com.tapdata.processor.dataflow.aggregation.incr;

import com.tapdata.constant.TapdataOffset;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.dataflow.Aggregation;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.DataFlowProcessor;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.aggregation.incr.cache.BucketCache;
import com.tapdata.processor.dataflow.aggregation.incr.cache.BucketCacheBuilder;
import com.tapdata.processor.dataflow.aggregation.incr.convert.MessageConverters;
import com.tapdata.processor.dataflow.aggregation.incr.convert.impl.DeleteMessageConverter;
import com.tapdata.processor.dataflow.aggregation.incr.convert.impl.InsertMessageConverter;
import com.tapdata.processor.dataflow.aggregation.incr.convert.impl.UpdateMessageConverter;
import com.tapdata.processor.dataflow.aggregation.incr.func.AggrFunction;
import com.tapdata.processor.dataflow.aggregation.incr.func.FuncCacheKey;
import com.tapdata.processor.dataflow.aggregation.incr.func.FunctionFactory;
import com.tapdata.processor.dataflow.aggregation.incr.service.AggregationService;
import com.tapdata.processor.dataflow.aggregation.incr.service.LifeCycleService;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotService;
import com.tapdata.processor.dataflow.aggregation.incr.service.SyncVersionService;
import com.tapdata.processor.dataflow.aggregation.incr.service.factory.ServiceFactories;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.BucketValue;
import com.tapdata.processor.dataflow.aggregation.incr.task.CleanScheduler;
import com.tapdata.processor.dataflow.aggregation.incr.task.SyncScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class IncrementAggregationProcessor implements DataFlowProcessor {

	private static final Logger log = LogManager.getLogger(IncrementAggregationProcessor.class);

	private Stage stage;
	private ProcessorContext processorContext;

	private final Map<Class<?>, LifeCycleService> lifeCycleServiceMap = new HashMap<>();
	private Lock lock;
	private boolean hasResetSnapshot = false; // flag to reset snapshot data

	@Override
	public void initialize(ProcessorContext context, Stage stage) throws Exception {
		this.processorContext = context;
		this.stage = stage;
		try {
			this.doInit();
			log.info("job/stage [{}/{}] IncrementAggregationProcessor init success", processorContext.getJob().getId(), stage.getId());
		} catch (Throwable t) {
			log.error(String.format("job/stage [%s/%s] IncrementAggregationProcessor init fail", processorContext.getJob().getId(), stage.getId()), t);
			throw new RuntimeException(t);
		}
	}

	private void doInit() throws Throwable {
		this.lock = new ReentrantLock(true);
		// database service
		SnapshotService<?> snapshotService = ServiceFactories.create(SnapshotService.class, this.stage, this.processorContext);
		AggregationService aggregationService = ServiceFactories.create(AggregationService.class, this.stage, this.processorContext);
		SyncVersionService syncVersionService = ServiceFactories.create(SyncVersionService.class, this.stage, this.processorContext);
		snapshotService.start();
		aggregationService.start();
		syncVersionService.start();
		// cache
		final BucketCache<FuncCacheKey, BucketValue> bucketCache = new BucketCacheBuilder<FuncCacheKey, BucketValue>().maxSize(this.stage.getAggCacheMaxSize()).build();
		// function
		final List<AggrFunction> aggrFunctionList = new ArrayList<>(this.stage.getAggregations().size());
		for (Aggregation aggregation : this.stage.getAggregations()) {
			aggregation.setJsEngineName(this.stage.getJsEngineName());
			aggrFunctionList.add(FunctionFactory.getInstance().create(bucketCache, aggregation));
		}
		// message converter
		MessageConverters.register(new InsertMessageConverter(this.stage, aggrFunctionList, snapshotService, syncVersionService));
		MessageConverters.register(new UpdateMessageConverter(this.stage, aggrFunctionList, snapshotService, syncVersionService));
		MessageConverters.register(new DeleteMessageConverter(this.stage, aggrFunctionList, snapshotService, syncVersionService));
		// scheduler
		SyncScheduler syncScheduler = new SyncScheduler(this.processorContext, this.stage, snapshotService, syncVersionService, aggrFunctionList, bucketCache, lock);
		CleanScheduler cleanScheduler = new CleanScheduler(this.stage, aggregationService, syncVersionService);
		syncScheduler.start();
		cleanScheduler.start();
		// LifeCycle control
		lifeCycleServiceMap.put(SnapshotService.class, snapshotService);
		lifeCycleServiceMap.put(AggregationService.class, aggregationService);
		lifeCycleServiceMap.put(SyncVersionService.class, syncVersionService);
		lifeCycleServiceMap.put(CleanScheduler.class, cleanScheduler);
		lifeCycleServiceMap.put(SyncScheduler.class, syncScheduler);
	}

	@Override
	public void stop() {
		Throwable throwable = null;
		for (LifeCycleService service : lifeCycleServiceMap.values()) {
			try {
				service.destroy();
				log.info("job/stage [{}/{}] IncrementAggregationProcessor stop {} success", this.processorContext.getJob().getId(), this.stage.getId(), service.getClass().getSimpleName());
			} catch (Throwable t) {
				log.info(String.format("job/stage [%s/%s] IncrementAggregationProcessor stop %s fail", this.processorContext.getJob().getId(), this.stage.getId(), service.getClass().getSimpleName()), t);
				if (throwable == null) {
					throwable = t;
				} else {
					throwable.addSuppressed(t);
				}
			}
		}
		if (throwable != null) {
			throw new RuntimeException(throwable);
		}
	}

	@Override
	public Stage getStage() {
		return this.stage;
	}

	@Override
	public List<MessageEntity> process(List<MessageEntity> messageEntityList) {
		try {
			lock.lock();
			this.maybeResetSnapshot(messageEntityList.get(0));
			return this.doProcess(messageEntityList);
		} finally {
			lock.unlock();
		}
	}

	private List<MessageEntity> doProcess(List<MessageEntity> messageEntityList) {
		List<MessageEntity> aggrEntityList = new ArrayList<>();
		for (MessageEntity e : messageEntityList) {
			Collection<MessageEntity> tempList = MessageConverters.ofOperation(e.getOp()).convert(e);
			if (tempList != null && !tempList.isEmpty()) {
				aggrEntityList.addAll(tempList);
			}
		}
		if (aggrEntityList.isEmpty()) {
			MessageEntity last = messageEntityList.get(messageEntityList.size() - 1);
			MessageEntity commit = new MessageEntity(OperationType.COMMIT_OFFSET.getOp(), null, last.getTableName());
			commit.setOffset(last.getOffset());
			aggrEntityList.add(commit);
		}
		return aggrEntityList;
	}

	private <T> T getService(Class<T> cls) {
		return (T) lifeCycleServiceMap.get(cls);
	}

	private void maybeResetSnapshot(MessageEntity messageEntity) {
		if (hasResetSnapshot) {
			return;
		}
		SnapshotService<?> snapshotService = this.getService(SnapshotService.class);
		Object offset = messageEntity.getOffset();
		if (offset instanceof TapdataOffset) {
			TapdataOffset tapdataOffset = (TapdataOffset) offset;
			if (TapdataOffset.SYNC_STAGE_SNAPSHOT.equals(tapdataOffset.getSyncStage())) {
				log.info("prepare to reset snapshot data");
				snapshotService.reset();
			}
		} else if (offset instanceof Map) {
			if (((Map) offset).isEmpty()) {
				log.info("prepare to reset snapshot data");
				snapshotService.reset();
			}
		} else if (offset == null) {
			log.info("prepare to reset snapshot data");
			snapshotService.reset();
		}
		hasResetSnapshot = true;
	}

}
