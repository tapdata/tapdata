package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.google.common.collect.Queues;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MigrateProcessorNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.ProcessorNodeProcessAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.concurrent.SimpleConcurrentProcessorImpl;
import io.tapdata.common.concurrent.TapExecutors;
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
import org.apache.commons.collections4.ListUtils;
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
	protected EventBatchProcessor batchProcessor;
	private Boolean enableConcurrentProcess;
	private int concurrentNum;
	private SimpleConcurrentProcessorImpl<List<BatchEventWrapper>, List<TapdataEvent>> simpleConcurrentProcessor;
	private int concurrentBatchSize;

	public HazelcastProcessorBaseNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		Node<?> node = processorBaseContext.getNode();
		String tag = node.getId() + "-" + node.getName();
		this.delayHandler = new DelayHandler(obsLogger, tag);
		initConcurrentExecutor();
		initBatchProcessorIfNeed();
	}

	protected void initConcurrentExecutor() {
		Node<?> node = getNode();
		TaskDto taskDto = processorBaseContext.getTaskDto();
		if (node instanceof ProcessorNode) {
			enableConcurrentProcess = ((ProcessorNode) node).getEnableConcurrentProcess();
			concurrentNum = ((ProcessorNode) node).getConcurrentNum();
		} else if (node instanceof MigrateProcessorNode) {
			enableConcurrentProcess = ((MigrateProcessorNode) node).getEnableConcurrentProcess();
			concurrentNum = ((MigrateProcessorNode) node).getConcurrentNum();
		}
		if (concurrentNum <= 1) {
			enableConcurrentProcess = false;
		}
		if (Boolean.TRUE.equals(enableConcurrentProcess)) {
			if (!supportConcurrentProcess()) {
				obsLogger.info("Node {}({}: {}) enable concurrent process, but not support concurrent process, disable concurrent process", getNode().getType(), getNode().getName(), getNode().getId());
				enableConcurrentProcess = false;
			} else if (TaskDto.SYNC_TYPE_TEST_RUN.equals(taskDto.getSyncType()) || TaskDto.SYNC_TYPE_DEDUCE_SCHEMA.equals(taskDto.getSyncType())) {
				enableConcurrentProcess = false;
			} else {
				concurrentBatchSize = Math.max(1, DEFAULT_BATCH_SIZE / concurrentNum);
				simpleConcurrentProcessor = TapExecutors.createSimple(concurrentNum, concurrentBatchSize, TAG);
				obsLogger.info("Node {}({}: {}) enable concurrent process, concurrent num: {}", getNode().getType(), getNode().getName(), getNode().getId(), concurrentNum);
			}
		}
	}

	private void initBatchProcessorIfNeed() {
		if (!supportBatchProcess()) {
			if (Boolean.TRUE.equals(enableConcurrentProcess)) {
				obsLogger.info("Node {}({}: {}) enable concurrent process, but not support batch process, disable concurrent process", getNode().getType(), getNode().getName(), getNode().getId());
			}
			return;
		}
		this.batchProcessor = new EventBatchProcessor(getNode(), ibp -> {
			try {
				while (isRunning()) {
					List<BatchEventWrapper> drainEvents = new ArrayList<>();
					int drain = ibp.drainTo(drainEvents);
					if (drain <= 0) {
						continue;
					}

					if (Boolean.TRUE.equals(enableConcurrentProcess)) {
						ListUtils.partition(drainEvents, concurrentBatchSize).forEach(e -> simpleConcurrentProcessor.runAsync(e, this::batchProcess));
					} else {
						List<TapdataEvent> tapdataEvents = new ArrayList<>(batchProcess(drainEvents));
						enqueue(tapdataEvents);
					}

				}
			} catch (Exception e) {
				errorHandle(e);
			}
		});
		if (Boolean.TRUE.equals(enableConcurrentProcess)) {
			batchProcessor.startConcurrentConsumer(() -> {
				while (isRunning()) {
					List<TapdataEvent> tapdataEvents = simpleConcurrentProcessor.get();
					if (null == tapdataEvents) {
						continue;
					}
					enqueue(tapdataEvents);
				}
			});
		}
		Optional.ofNullable(obsLogger).ifPresent(log -> log.info("Node {}({}: {}) enable batch process", getNode().getType(), getNode().getName(), getNode().getId()));
	}

	private void enqueue(List<TapdataEvent> tapdataEvents) {
		for (TapdataEvent tapdataEvent : tapdataEvents) {
			while (isRunning()) {
				if (delayHandler.process(() -> this.offer(tapdataEvent))) {
					break;
				}
			}
		}
	}

	private List<TapdataEvent> batchProcess(List<BatchEventWrapper> batchEventWrappers) {
		List<TapdataEvent> result = new ArrayList<>();
		for (BatchEventWrapper batchEventWrapper : batchEventWrappers) {
			TapdataEvent tapdataEvent = batchEventWrapper.getTapdataEvent();
			if (tapdataEvent.isDML() && needTransformValue()) {
				batchEventWrapper.tapValueTransform = transformFromTapValue(tapdataEvent);
			}
		}
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
						if (needTransformValue()) {
							if (null != processResult.getTableId()) {
								transformToTapValue(tapdataEvent, processorBaseContext.getTapTableMap(), processResult.getTableId(), tapValueTransform);
							} else {
								transformToTapValue(tapdataEvent, processorBaseContext.getTapTableMap(), getNode().getId(), tapValueTransform);
							}
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
		batchEventWrappers.forEach(cbe -> {
			if (null != cbe.getProcessAspect()) {
				AspectUtils.accept(cbe.getProcessAspect().state(ProcessorNodeProcessAspect.STATE_PROCESSING).getConsumers(), cbe.getTapdataEvent());
			}
		});
		return result;
	}

	@Override
	protected TapCodecsFilterManager initFilterCodec() {
		TapCodecsRegistry tapCodecsRegistry = TapCodecsRegistry.create();
		TapCodecUtil.registerToTapValue(tapCodecsRegistry);
		return TapCodecsFilterManager.create(tapCodecsRegistry);
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
							if (supportBatchProcess()) {
								while (isRunning()) {
									try {
										if (batchProcessor.offer(new BatchEventWrapper(tapdataEvent, processorNodeProcessAspect))) {
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
						if (supportBatchProcess()) {
							if (batchProcessor.status == EventBatchProcessor.NOT_RUN) {
								batchProcessor.running();
							}
							while (isRunning()) {
								try {
									if (batchProcessor.offer(new BatchEventWrapper(tapdataEvent, processorNodeProcessAspect))) {
										break;
									}
								} catch (InterruptedException e) {
									break;
								}
							}
						} else {
							AtomicReference<TapValueTransform> tapValueTransform = new AtomicReference<>();
							if (tapdataEvent.isDML() && needTransformValue()) {
								tapValueTransform.set(transformFromTapValue(tapdataEvent));
							}
							handleOriginalValueMapIfNeed(tapValueTransform);
							tryProcess(tapdataEvent, (event, processResult) -> {
								if (null == event) {
									return;
								}
								if (tapdataEvent.isDML()) {
									if (processResult == null) {
										processResult = getProcessResult(TapEventUtil.getTableId(tapdataEvent.getTapEvent()));
									}
									if (needTransformValue()) {
										if (null != processResult.getTableId()) {
											transformToTapValue(event, processorBaseContext.getTapTableMap(), processResult.getTableId(), tapValueTransform.get());
										} else {
											transformToTapValue(event, processorBaseContext.getTapTableMap(), getNode().getId(), tapValueTransform.get());
										}
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
				enqueue(processedEventList);
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

	protected boolean supportBatchProcess() {
		return true;
	}

	@Override
	protected void doClose() throws TapCodeException {
		try {
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.batchProcessor).ifPresent(EventBatchProcessor::shutdown), TAG);
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.simpleConcurrentProcessor).ifPresent(SimpleConcurrentProcessorImpl::close), TAG);
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
			if (controlOrIgnoreEvent(tapdataEvent)) {
				batchProcessResults.add(new BatchProcessResult(batchEventWrapper, null));
				continue;
			}
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

	public static class EventBatchProcessor {
		static final int NOT_RUN = 1;
		static final int RUNNING = 2;
		static final int FINISH = 3;
		int batchSize;
		long batchTimeoutMs;
		Node node;
		LinkedBlockingQueue<BatchEventWrapper> tapdataEventQueue;
		int status = 1;
		ExecutorService batchConsumerThreadPool;

		public EventBatchProcessor(Node node, Consumer<EventBatchProcessor> batchProcessor) {
			this.node = node;
			this.batchConsumerThreadPool = new ThreadPoolExecutor(1, 2, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
			this.batchConsumerThreadPool.submit(() -> {
				Thread.currentThread().setName(String.join("-", TAG, "processor-node-batch-consumer-thread", node.getId()));
				batchProcessor.accept(this);
			});
			this.batchSize = CommonUtils.getPropertyInt(PROCESSOR_BATCH_SIZE_PROP_KEY, DEFAULT_BATCH_SIZE);
			this.batchTimeoutMs = CommonUtils.getPropertyLong(PROCESSOR_BATCH_TIMEOUT_MS_PROP_KEY, DEFAULT_BATCH_TIMEOUT_MS);
			this.tapdataEventQueue = new LinkedBlockingQueue<>(batchSize * 2);
		}

		public void startConcurrentConsumer(Runnable runnable) {
			this.batchConsumerThreadPool.submit(() -> {
				Thread.currentThread().setName(String.join("-", TAG, "processor-node-concurrent-consumer-thread", node.getId()));
				runnable.run();
			});
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
			if (null == batchEventWrapper) {
				return true;
			}
			return tapdataEventQueue.offer(batchEventWrapper, TimeUnit.SECONDS.toMillis(1L), TimeUnit.MILLISECONDS);
		}

		int drainTo(List<BatchEventWrapper> tapdataEvents) {
			try {
				return Queues.drain(tapdataEventQueue, tapdataEvents, batchSize, batchTimeoutMs, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | NullPointerException ignored) {
			}
			return 0;
		}

		void shutdown() {
			finish();
			Optional.ofNullable(this.batchConsumerThreadPool).ifPresent(ExecutorService::shutdownNow);
		}
	}

	protected static class BatchEventWrapper {
		private TapdataEvent tapdataEvent;
		private TapValueTransform tapValueTransform;
		private ProcessorNodeProcessAspect processAspect;

		public BatchEventWrapper(TapdataEvent tapdataEvent) {
			this.tapdataEvent = tapdataEvent;
		}

		public BatchEventWrapper(TapdataEvent tapdataEvent, ProcessorNodeProcessAspect processAspect) {
			this.tapdataEvent = tapdataEvent;
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

	public boolean needTransformValue() {
		return true;
	}

	public boolean supportConcurrentProcess() {
		return false;
	}
}
