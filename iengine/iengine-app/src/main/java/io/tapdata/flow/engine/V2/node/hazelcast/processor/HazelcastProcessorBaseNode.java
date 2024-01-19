package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.google.common.collect.Queues;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.ProcessorNodeProcessAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.util.DelayHandler;
import io.tapdata.flow.engine.V2.util.TapCodecUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-12 17:10
 **/
public abstract class HazelcastProcessorBaseNode extends HazelcastBaseNode {
	private static final String TAG = HazelcastProcessorBaseNode.class.getSimpleName();
	public static final String PROCESSOR_BATCH_SIZE_PROP_KEY = "PROCESSOR_BATCH_SIZE";
	public static final String PROCESSOR_BATCH_TIMEOUT_MS_PROP_KEY = "PROCESSOR_BATCH_TIMEOUT_MS";
	public static final int DEFAULT_BATCH_SIZE = 1000;
	public static final long DEFAULT_BATCH_TIMEOUT_MS = 1000L;

	/**
	 * Ignore process
	 */
	private boolean ignore;

	private DelayHandler delayHandler;

	protected SyncStage syncStage;
	private boolean enableBatch = false;
	protected EventBatchProcessor batchProcessor;

	public HazelcastProcessorBaseNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		Node<?> node = processorBaseContext.getNode();
		String tag = node.getId() + "-" + node.getName();
		this.delayHandler = new DelayHandler(obsLogger, tag);
		initEnableInitialBatch();
		initBatchProcessorIfNeed();
	}

	private void initEnableInitialBatch() {
		Node node = getNode();
		if (node instanceof MergeTableNode) {
			enableBatch = true;
		}
	}

	private void initBatchProcessorIfNeed() {
		if (!this.enableBatch) {
			return;
		}
		this.batchProcessor = new EventBatchProcessor(getNode(), ibp -> {
			try {
				List<TapdataEvent> tapdataEvents = new ArrayList<>();
				List<BatchEventWrapper> cacheBatchEvents = new ArrayList<>();
				while (isRunning()) {
					List<BatchEventWrapper> batchEventWrappers = ibp.drainTo();
					if (CollectionUtils.isEmpty(batchEventWrappers)) {
						continue;
					}
					for (BatchEventWrapper batchEventWrapper : batchEventWrappers) {
						TapdataEvent tapdataEvent = batchEventWrapper.getTapdataEvent();
						if (null == tapdataEvent) {
							continue;
						}
						if (controlOrIgnoreEvent(tapdataEvent)) {
							batchProcess(cacheBatchEvents, tapdataEvents);
							tapdataEvents.add(tapdataEvent);
						} else {
							cacheBatchEvents.add(batchEventWrapper);
						}
					}
					batchProcess(cacheBatchEvents, tapdataEvents);

					for (TapdataEvent tapdataEvent : tapdataEvents) {
						while (isRunning()) {
							if (offer(tapdataEvent)) {
								break;
							}
						}
					}
					tapdataEvents.clear();
				}
			} catch (Exception e) {
				errorHandle(e);
			}
		});
		obsLogger.info("Node %s(%s) enable initial batch", getNode().getId(), getNode().getName());
	}

	private void batchProcess(List<BatchEventWrapper> cacheBatchEvents, List<TapdataEvent> tapdataEvents) {
		if (CollectionUtils.isNotEmpty(cacheBatchEvents)) {
			tapdataEvents.addAll(batchProcess(cacheBatchEvents));
			cacheBatchEvents.forEach(cbe -> {
				if (null != cbe.getProcessAspect()) {
					AspectUtils.accept(cbe.getProcessAspect().state(ProcessorNodeProcessAspect.STATE_PROCESSING).getConsumers(), cbe.getTapdataEvent());
				}
			});
			cacheBatchEvents.clear();
		}
	}

	private List<TapdataEvent> batchProcess(List<BatchEventWrapper> batchEventWrappers) {
		List<TapdataEvent> result = new ArrayList<>();
		tryProcess(batchEventWrappers, processResults -> {
			if (CollectionUtils.isEmpty(processResults)) {
				return;
			}
			for (BatchProcessResult batchProcessResult : processResults) {
				ProcessResult processResult = batchProcessResult.getProcessResult();
				BatchEventWrapper batchEventWrapper = batchProcessResult.getBatchEventWrapper();
				TapdataEvent tapdataEvent = batchEventWrapper.getTapdataEvent();
				TapValueTransform tapValueTransform = batchEventWrapper.getTapValueTransform();

				if (null == tapdataEvent) {
					return;
				}
				if (tapdataEvent.isDML()) {
					if (processResult == null) {
						processResult = getProcessResult(TapEventUtil.getTableId(tapdataEvent.getTapEvent()));
					}
					try {
						if (null != processResult.getTableId()) {
							transformToTapValue(tapdataEvent, processorBaseContext.getTapTableMap(), processResult.getTableId(), tapValueTransform);
						} else {
							transformToTapValue(tapdataEvent, processorBaseContext.getTapTableMap(), getNode().getId(), tapValueTransform);
						}
					} catch (Exception e) {
						if (isRunning()) {
							throw e;
						}
					}
				}

				result.add(tapdataEvent);
			}
		});
		return result;
	}

	@Override
	protected TapCodecsFilterManager initFilterCodec() {
		TapCodecsRegistry tapCodecsRegistry = TapCodecsRegistry.create();
		return TapCodecUtil.getCodecsFilterManager(tapCodecsRegistry);
	}

	@Override
	protected final boolean tryProcess(int ordinal, @NotNull Object item) throws Exception {
		AtomicBoolean result = new AtomicBoolean(true);
		try {
			if (!isRunning()) {
				return true;
			}
			TapdataEvent tapdataEvent = (TapdataEvent) item;
			List<TapdataEvent> processedEventList = new ArrayList<>();
			if (!getNode().disabledNode()) {
				try {
					AspectUtils.executeProcessorFuncAspect(ProcessorNodeProcessAspect.class, () -> new ProcessorNodeProcessAspect()
							.processorBaseContext(getProcessorBaseContext())
							.inputEvent(tapdataEvent)
							.start(), processorNodeProcessAspect -> {
						if (null != tapdataEvent.getSyncStage()) {
							syncStage = tapdataEvent.getSyncStage();
						}
						if (controlOrIgnoreEvent(tapdataEvent)) {
							if (needBatchProcess()) {
								while (isRunning()) {
									try {
										if (batchProcessor.offer(new BatchEventWrapper(tapdataEvent, null, processorNodeProcessAspect))) {
											break;
										}
									} catch (InterruptedException e) {
										break;
									}
								}
							} else {
								// control tapdata event, skip the process consider process is done
								processedEventList.add(tapdataEvent);
								if (null != processorNodeProcessAspect) {
									AspectUtils.accept(processorNodeProcessAspect.state(ProcessorNodeProcessAspect.STATE_PROCESSING).getConsumers(), tapdataEvent);
								}
							}
							return;
						}
						// Update memory from ddl event info map
						updateMemoryFromDDLInfoMap(tapdataEvent);
						AtomicReference<TapValueTransform> tapValueTransform = new AtomicReference<>();
						if (tapdataEvent.isDML()) {
							tapValueTransform.set(transformFromTapValue(tapdataEvent));
						}
						if (needBatchProcess()) {
							if (batchProcessor.status == EventBatchProcessor.NOT_RUN) {
								batchProcessor.running();
							}
							while (isRunning()) {
								try {
									if (batchProcessor.offer(new BatchEventWrapper(tapdataEvent, tapValueTransform.get(), processorNodeProcessAspect))) {
										break;
									}
								} catch (InterruptedException e) {
									break;
								}
							}
						} else {
							handleOriginalValueMapIfNeed(tapValueTransform);
							tryProcess(tapdataEvent, (event, processResult) -> {
								if (null == event) {
									return;
								}
								if (tapdataEvent.isDML()) {
									if (processResult == null) {
										processResult = getProcessResult(TapEventUtil.getTableId(tapdataEvent.getTapEvent()));
									}
									if (null != processResult.getTableId()) {
										transformToTapValue(event, processorBaseContext.getTapTableMap(), processResult.getTableId(), tapValueTransform.get());
									} else {
										transformToTapValue(event, processorBaseContext.getTapTableMap(), getNode().getId(), tapValueTransform.get());
									}
								}

								// consider process is done
								processedEventList.add(event);
								if (null != processorNodeProcessAspect) {
									AspectUtils.accept(processorNodeProcessAspect.state(ProcessorNodeProcessAspect.STATE_PROCESSING).getConsumers(), event);
								}
							});
						}
					});
				} catch (Throwable throwable) {
					if (throwable instanceof TapCodeException) {
						throw (TapCodeException) throwable;
					} else {
						throw new TapEventException(TaskProcessorExCode_11.UNKNOWN_ERROR, throwable).addEvent(tapdataEvent.getTapEvent());
					}
				}
			} else {
				processedEventList.add(tapdataEvent);
			}

			if (CollectionUtils.isNotEmpty(processedEventList)) {
				for (TapdataEvent event : processedEventList) {
					while (isRunning()) {
						if (delayHandler.process(() -> this.offer(event))) {
							break;
						}
					}
				}
			}
		} catch (Throwable throwable) {
			errorHandle(throwable, throwable.getMessage());
		}
		return result.get();
	}

	protected void handleOriginalValueMapIfNeed(AtomicReference<TapValueTransform> tapValueTransform) {
		// do nothing
	}

	private boolean controlOrIgnoreEvent(TapdataEvent tapdataEvent) {
		return null == tapdataEvent.getTapEvent() || ignore;
	}

	private boolean needBatchProcess() {
		return enableBatch && null != batchProcessor;
	}

	private boolean waitBatchFinishIfNeed() {
		return null != batchProcessor && batchProcessor.status == EventBatchProcessor.RUNNING;
	}

	@Override
	protected void doClose() throws TapCodeException {
		try {
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.batchProcessor).ifPresent(EventBatchProcessor::shutdown), TAG);
		} finally {
			super.doClose();
		}
	}

	protected ProcessResult getProcessResult(String tableName) {
		if (!multipleTables && !StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA)) {
			tableName = processorBaseContext.getNode().getId();
		}
		if (StringUtils.isEmpty(tableName)) {
			tableName = null;
		}
		return ProcessResult.create().tableId(tableName);
	}

	protected abstract void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer);

	protected void tryProcess(List<BatchEventWrapper> tapdataEvents, Consumer<List<BatchProcessResult>> consumer) {
		if (null == tapdataEvents) {
			return;
		}
		List<BatchProcessResult> batchProcessResults = new ArrayList<>();
		for (BatchEventWrapper batchEventWrapper : tapdataEvents) {
			TapdataEvent tapdataEvent = batchEventWrapper.getTapdataEvent();
			tryProcess(tapdataEvent, (event, processResult) -> {
				if (null == event) {
					return;
				}
				if (tapdataEvent.isDML()) {
					if (processResult == null) {
						processResult = getProcessResult(TapEventUtil.getTableId(tapdataEvent.getTapEvent()));
					}
				}
				batchEventWrapper.setTapdataEvent(event);
				BatchProcessResult batchProcessResult = new BatchProcessResult(batchEventWrapper, processResult);
				batchProcessResults.add(batchProcessResult);
			});
		}
		consumer.accept(batchProcessResults);
	}

	protected void setIgnore(boolean ignore) {
		this.ignore = ignore;
	}

	protected static class ProcessResult {
		private String tableId;

		public static ProcessResult create() {
			return new ProcessResult();
		}

		public ProcessResult tableId(String tableId) {
			this.tableId = tableId;
			return this;
		}

		public String getTableId() {
			return tableId;
		}
	}

	protected static class BatchProcessResult {
		private BatchEventWrapper batchEventWrapper;
		private ProcessResult processResult;

		public BatchProcessResult(BatchEventWrapper batchEventWrapper, ProcessResult processResult) {
			this.batchEventWrapper = batchEventWrapper;
			this.processResult = processResult;
		}

		public BatchEventWrapper getBatchEventWrapper() {
			return batchEventWrapper;
		}

		public ProcessResult getProcessResult() {
			return processResult;
		}
	}

	private static class EventBatchProcessor {
		static final int NOT_RUN = 1;
		static final int RUNNING = 2;
		static final int FINISH = 3;
		int batchSize;
		long batchTimeoutMs;
		LinkedBlockingQueue<BatchEventWrapper> tapdataEventQueue;
		int status = 1;
		private Node node;
		ExecutorService batchConsumerThreadPool;

		public EventBatchProcessor(Node node, BatchProcessor batchProcessor) {
			this.node = node;
			this.batchConsumerThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), runnable -> {
				Thread thread = new Thread(runnable);
				thread.setName("processor-node-initial-batch-consumer-" + node.getId());
				return thread;
			});
			this.batchConsumerThreadPool.submit(() -> batchProcessor.process(this));
			this.batchSize = CommonUtils.getPropertyInt(PROCESSOR_BATCH_SIZE_PROP_KEY, DEFAULT_BATCH_SIZE);
			this.batchTimeoutMs = CommonUtils.getPropertyLong(PROCESSOR_BATCH_TIMEOUT_MS_PROP_KEY, DEFAULT_BATCH_TIMEOUT_MS);
			this.tapdataEventQueue = new LinkedBlockingQueue<>(batchSize * 2);
		}

		void notRun() {
			this.status = NOT_RUN;
		}

		void running() {
			this.status = RUNNING;
		}

		void finish() {
			this.status = FINISH;
		}

		boolean offer(BatchEventWrapper batchEventWrapper) throws InterruptedException {
			return tapdataEventQueue.offer(batchEventWrapper, TimeUnit.SECONDS.toMillis(1L), TimeUnit.MILLISECONDS);
		}

		List<BatchEventWrapper> drainTo() {
			List<BatchEventWrapper> tapdataEvents = new ArrayList<>();
			try {
				Queues.drain(tapdataEventQueue, tapdataEvents, batchSize, batchTimeoutMs, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ignored) {
			}
			return tapdataEvents;
		}

		void shutdown() {
			finish();
			Optional.ofNullable(this.batchConsumerThreadPool).ifPresent(ExecutorService::shutdownNow);
		}
	}

	private interface BatchProcessor {
		void process(EventBatchProcessor eventBatchProcessor);
	}

	protected static class BatchEventWrapper {
		private TapdataEvent tapdataEvent;
		private TapValueTransform tapValueTransform;
		private ProcessorNodeProcessAspect processAspect;

		public BatchEventWrapper(TapdataEvent tapdataEvent) {
			this.tapdataEvent = tapdataEvent;
		}

		public BatchEventWrapper(TapdataEvent tapdataEvent, TapValueTransform tapValueTransform, ProcessorNodeProcessAspect processAspect) {
			this.tapdataEvent = tapdataEvent;
			this.tapValueTransform = tapValueTransform;
			this.processAspect = processAspect;
		}

		public TapdataEvent getTapdataEvent() {
			return tapdataEvent;
		}

		public TapValueTransform getTapValueTransform() {
			return tapValueTransform;
		}

		public void setTapdataEvent(TapdataEvent tapdataEvent) {
			this.tapdataEvent = tapdataEvent;
		}

		public ProcessorNodeProcessAspect getProcessAspect() {
			return processAspect;
		}
	}
}
