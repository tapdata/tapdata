package io.tapdata.flow.engine.V2.node.hazelcast;

import cn.hutool.core.date.StopWatch;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.Stats;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.dataflow.RuntimeThroughput;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.SettingService;
import io.tapdata.entity.OnData;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.error.TapProcessorUnknownException;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.monitor.impl.JetJobStatusMonitor;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation.HazelcastMultiAggregatorProcessor;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.TapCodecUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import io.tapdata.websocket.handler.TestRunTaskHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jackin
 * @date 2021/12/7 3:25 PM
 **/
public abstract class HazelcastBaseNode extends AbstractProcessor {
	public static final String TARGET_TAG = "target";
	public static final String SOURCE_TAG = "source";
	/**
	 * [sub task id]-[node id]
	 */
	protected static final String NEW_DAG_INFO_KEY = "NEW_DAG";
	protected static final String DAG_DATA_SERVICE_INFO_KEY = "DAG_DATA_SERVICE";
	protected static final String TRANSFORM_SCHEMA_ERROR_MESSAGE_INFO_KEY = "TRANSFORM_SCHEMA_ERROR_MESSAGE";
	protected static final String UPDATE_METADATA_INFO_KEY = "UPDATE_METADATA";
	protected static final String INSERT_METADATA_INFO_KEY = "INSERT_METADATA";
	protected static final String REMOVE_METADATA_INFO_KEY = "REMOVE_METADATA";
	protected static final String QUALIFIED_NAME_ID_MAP_INFO_KEY = "QUALIFIED_NAME_ID_MAP";
	private static final String TAG = HazelcastBaseNode.class.getSimpleName();

	protected ClientMongoOperator clientMongoOperator;
	protected Context jetContext;
	protected SettingService settingService;

	protected Map<String, String> tags;
	protected TapCodeException error;
	protected String errorMessage;
	protected ProcessorBaseContext processorBaseContext;

	public AtomicBoolean running = new AtomicBoolean(false);
	protected TapCodecsFilterManager codecsFilterManager;

	/**
	 * Whether to process data from multiple tables
	 */
	protected boolean multipleTables;

	protected ObsLogger obsLogger;
	protected MonitorManager monitorManager;
	private JetJobStatusMonitor jetJobStatusMonitor;
	protected String lastTableName;
	protected ExternalStorageDto externalStorageDto;

	protected HazelcastBaseNode(ProcessorBaseContext processorBaseContext) {
		this.processorBaseContext = processorBaseContext;
	}

	public <T extends DataFunctionAspect<T>> AspectInterceptResult executeDataFuncAspect(Class<T> aspectClass, Callable<T> aspectCallable, CommonUtils.AnyErrorConsumer<T> anyErrorConsumer) {
		return AspectUtils.executeDataFuncAspect(aspectClass, aspectCallable, anyErrorConsumer);
	}

	public <T extends Aspect> AspectInterceptResult executeAspect(Class<T> aspectClass, Callable<T> aspectCallable) {
		return AspectUtils.executeAspect(aspectClass, aspectCallable);
	}

	public <T extends Aspect> AspectInterceptResult executeAspect(T aspect) {
		return AspectUtils.executeAspect(aspect);
	}

	protected void doInit(@NotNull Processor.Context context) throws TapCodeException {
	}

	protected void doInitWithDisableNode(@NotNull Context context) throws TapCodeException {
	}

	@Override
	public final void init(@NotNull Processor.Context context) throws Exception {
		try {
			this.jetContext = context;
			super.init(context);
			this.running.compareAndSet(false, true);
			this.obsLogger = initObsLogger();
			if (null != processorBaseContext.getConfigurationCenter()) {
				this.clientMongoOperator = initClientMongoOperator();
				this.settingService = initSettingService();
			} else {
				throw new TapCodeException(TaskProcessorExCode_11.INIT_CONFIGURATION_CENTER_CANNOT_BE_NULL);
			}
			if (null != processorBaseContext.getNode() && null == processorBaseContext.getNode().getGraph()) {
				Dag dag = new Dag(processorBaseContext.getEdges(), processorBaseContext.getNodes());
				DAG _DAG = DAG.build(dag);
				_DAG.setTaskId(processorBaseContext.getTaskDto().getId());
				processorBaseContext.getTaskDto().setDag(_DAG);
			}

			// 如果为迁移任务、且源节点为数据库类型
			this.multipleTables = CollectionUtils.isNotEmpty(processorBaseContext.getTaskDto().getDag().getSourceNode());

			// Init external storage config
			this.externalStorageDto = initExternalStorage();
			this.codecsFilterManager = initFilterCodec();
			// Execute ProcessorNodeInitAspect before doInit since we need to init the aspect first
			executeAspectOnInit();
			this.monitorManager = initMonitor();
			startMonitorIfNeed(context);
			setThreadName();
			if (!getNode().disabledNode()) {
				doInit(context);
			} else {
				doInitWithDisableNode(context);
			}
		} catch (Exception e) {
			errorHandle(e);
		}
	}

	protected MonitorManager initMonitor() {
		return MonitorManager.create();
	}

	protected void startMonitorIfNeed(@NotNull Context context) {
		if (!StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(),
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
			try {
				monitorManager.startMonitor(MonitorManager.MonitorType.JET_JOB_STATUS_MONITOR, context.hazelcastInstance().getJet().getJob(context.jobId()), processorBaseContext.getNode().getId());
			} catch (Exception e) {
				throw new TapCodeException(TaskProcessorExCode_11.START_JET_JOB_STATUS_MONITOR_FAILED, e);
			}
			jetJobStatusMonitor = (JetJobStatusMonitor) monitorManager.getMonitorByType(MonitorManager.MonitorType.JET_JOB_STATUS_MONITOR);
		}
	}

	protected void executeAspectOnInit() {
		if (this instanceof HazelcastProcessorBaseNode || this instanceof HazelcastMultiAggregatorProcessor) {
			AspectUtils.executeAspect(ProcessorNodeInitAspect.class, () -> new ProcessorNodeInitAspect().processorBaseContext(processorBaseContext));
		} else {
			AspectUtils.executeAspect(DataNodeInitAspect.class, () -> new DataNodeInitAspect().dataProcessorContext((DataProcessorContext) processorBaseContext));
		}
	}

	protected ExternalStorageDto initExternalStorage() {
		return ExternalStorageUtil.getExternalStorage(
				processorBaseContext.getTaskConfig().getExternalStorageDtoMap(),
				processorBaseContext.getNode(),
				clientMongoOperator,
				processorBaseContext.getNodes(),
				(processorBaseContext instanceof DataProcessorContext ? ((DataProcessorContext) processorBaseContext).getConnections() : null)
		);
	}

	protected SettingService initSettingService() {
		if (null == clientMongoOperator) {
			throw new TapCodeException(TaskProcessorExCode_11.INIT_SETTING_SERVICE_FAILED_CLIENT_MONGO_OPERATOR_IS_NULL);
		}
		return new SettingService(clientMongoOperator);
	}

	protected ClientMongoOperator initClientMongoOperator() {
		return BeanUtil.getBean(ClientMongoOperator.class);
	}

	protected ObsLogger initObsLogger() {
		return ObsLoggerFactory.getInstance().getObsLogger(
				processorBaseContext.getTaskDto(),
				processorBaseContext.getNode().getId(),
				processorBaseContext.getNode().getName()
		);
	}

	protected void setThreadName() {
		TaskDto taskDto = processorBaseContext.getTaskDto();
		Node<?> node = getNode();
		Thread.currentThread().setName(String.format("%s-%s(%s)-%s(%s)", TAG, taskDto.getName(), taskDto.getId().toHexString(), node.getName(), node.getId()));
	}


	public ProcessorBaseContext getProcessorBaseContext() {
		return processorBaseContext;
	}

	protected TapCodecsFilterManager initFilterCodec() {
		return TapCodecUtil.genericCodecsFilterManager();
	}

	protected TapValueTransform transformFromTapValue(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) return null;
		if (null == tapdataEvent.getTapEvent()) return null;
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		TapValueTransform tapValueTransform = TapValueTransform.create();
		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		if (MapUtils.isNotEmpty(before)) {
			tapValueTransform.before(codecsFilterManager.transformFromTapValueMap(before));
		}
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (MapUtils.isNotEmpty(after)) {
			tapValueTransform.after(codecsFilterManager.transformFromTapValueMap(after));
		}
		return tapValueTransform;
	}

	protected void transformToTapValue(TapdataEvent tapdataEvent, TapTableMap<String, TapTable> tapTableMap, String tableName) {
		transformToTapValue(tapdataEvent, tapTableMap, tableName, null);
	}

	protected void transformToTapValue(TapdataEvent tapdataEvent, TapTableMap<String, TapTable> tapTableMap, String tableName, TapValueTransform tapValueTransform) {
		if (!(tapdataEvent.getTapEvent() instanceof TapRecordEvent)) return;
		if (null == tapTableMap)
			throw new IllegalArgumentException("Transform to TapValue failed, tapTableMap is empty, table name: " + tableName);
		TapTable tapTable = tapTableMap.get(tableName);
		if (null == tapTable)
			throw new IllegalArgumentException("Transform to TapValue failed, table schema is empty, table name: " + tableName);
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		if (nameFieldMap == null)
			throw new IllegalArgumentException("Transform to TapValue failed, field map is empty, table name: " + tableName);
		TapEvent tapEvent = tapdataEvent.getTapEvent();

		Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (null != tapValueTransform) {
			if (MapUtils.isNotEmpty(before))
				codecsFilterManager.transformToTapValueMap(before, nameFieldMap, tapValueTransform.getBefore());
			if (MapUtils.isNotEmpty(after))
				codecsFilterManager.transformToTapValueMap(after, nameFieldMap, tapValueTransform.getAfter());
		} else {
			if (MapUtils.isNotEmpty(before)) codecsFilterManager.transformToTapValueMap(before, nameFieldMap);
			if (MapUtils.isNotEmpty(after)) codecsFilterManager.transformToTapValueMap(after, nameFieldMap);
		}
	}

	protected MessageEntity tapEvent2Message(TapRecordEvent dataEvent) {
		if (null == dataEvent) return null;
		MessageEntity messageEntity = new MessageEntity();
		Map<String, Object> before = TapEventUtil.getBefore(dataEvent);
		messageEntity.setBefore(before);
		Map<String, Object> after = TapEventUtil.getAfter(dataEvent);
		messageEntity.setAfter(after);
		messageEntity.setOp(TapEventUtil.getOp(dataEvent));
		messageEntity.setTableName(dataEvent.getTableId());
		messageEntity.setTimestamp(dataEvent.getReferenceTime());
		messageEntity.setTime(dataEvent.getTime());
		messageEntity.setInfo(dataEvent.getInfo());
		return messageEntity;
	}

	protected TapRecordEvent message2TapEvent(MessageEntity messageEntity) {
		if (null == messageEntity) return null;
		TapRecordEvent tapRecordEvent;
		String op = messageEntity.getOp();
		OperationType operationType = OperationType.fromOp(op);
		if (operationType == null) {
			throw new IllegalArgumentException(String.format("Unrecognized op type: %s", op));
		}
		switch (operationType) {
			case INSERT:
				tapRecordEvent = new TapInsertRecordEvent();
				((TapInsertRecordEvent) tapRecordEvent).setAfter(messageEntity.getAfter());
				break;
			case UPDATE:
				tapRecordEvent = new TapUpdateRecordEvent();
				((TapUpdateRecordEvent) tapRecordEvent).setBefore(messageEntity.getBefore());
				((TapUpdateRecordEvent) tapRecordEvent).setAfter(messageEntity.getAfter());
				break;
			case DELETE:
				tapRecordEvent = new TapDeleteRecordEvent();
				((TapDeleteRecordEvent) tapRecordEvent).setBefore(messageEntity.getBefore());
				break;
			default:
				tapRecordEvent = null;
				break;
		}
		if (null != tapRecordEvent) {
			tapRecordEvent.setTableId(messageEntity.getTableName());
			tapRecordEvent.setReferenceTime(messageEntity.getTimestamp());
			tapRecordEvent.setTime(messageEntity.getTime());
			tapRecordEvent.setInfo(messageEntity.getInfo());
		}
		return tapRecordEvent;
	}

	protected String getTableName(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) return "";
		MessageEntity messageEntity = tapdataEvent.getMessageEntity();
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		if (null != messageEntity) {
			return messageEntity.getTableName();
		} else {
			if (tapEvent instanceof TapBaseEvent) {
				return ((TapBaseEvent) tapEvent).getTableId();
			} else {
				return "";
			}
		}
	}

	protected int bucketIndex = 0;

	protected boolean offer(TapdataEvent dataEvent) {
		if (dataEvent != null) {
			if (processorBaseContext.getNode() != null) {
				dataEvent.addNodeId(processorBaseContext.getNode().getId());
			}
			Outbox outbox = getOutboxAndCheckNullable();
			int bucketCount = outbox.bucketCount();
			if (!tryEmit(dataEvent, bucketCount)) return false;
		}
		bucketIndex = 0; // reset to 0 of return true
		return true;
	}

	protected boolean tryEmit(TapdataEvent dataEvent, int bucketCount) {
		if (null == dataEvent) {
			return true;
		}
		if (bucketCount > 1) {
			for (bucketIndex = Math.min(bucketIndex, bucketCount); bucketIndex < bucketCount; bucketIndex++) {
				TapdataEvent cloneEvent = (TapdataEvent) dataEvent.clone();
				if (!tryEmit(bucketIndex, cloneEvent)) {
					return false;
				}
			}
		} else {
			return tryEmit(dataEvent);
		}
		return true;
	}

	@NotNull
	protected Outbox getOutboxAndCheckNullable() {
		Outbox outbox = getOutbox();
		if (null == outbox) {
			throw new TapCodeException(TaskProcessorExCode_11.OUTBOX_IS_NULL_WHEN_OFFER, "Get outbox failed, outbox is null");
		}
		return outbox;
	}

	protected void doClose() throws TapCodeException {
		CommonUtils.handleAnyError(() -> {
			Optional.ofNullable(processorBaseContext.getTapTableMap()).ifPresent(TapTableMap::reset);
			obsLogger.info(String.format("Node %s[%s] schema data cleaned", getNode().getName(), getNode().getId()));
		}, err -> obsLogger.warn(String.format("Clean node %s[%s] schema data failed: %s", getNode().getName(), getNode().getId(), err.getMessage())));
		CommonUtils.handleAnyError(() -> {
			if (this.monitorManager != null) {
				this.monitorManager.close();
			}
			obsLogger.info(String.format("Node %s[%s] monitor closed", getNode().getName(), getNode().getId()));
		}, err -> obsLogger.warn("Close monitor failed: " + err.getMessage()));
	}

	@Override
	public final void close() throws Exception {
		StopWatch sw = new StopWatch();
		try {
			sw.start();
			running.set(false);
			CommonUtils.ignoreAnyError(() -> {
				TaskClient<TaskDto> taskDtoTaskClient = BeanUtil.getBean(TapdataTaskScheduler.class).getTaskClientMap().get(processorBaseContext.getTaskDto().getId().toHexString());
				if (taskDtoTaskClient != null) {
					TaskDto taskDto = taskDtoTaskClient.getTask();
					processorBaseContext.getTaskDto().setSnapShotInterrupt(taskDto.isSnapShotInterrupt());
				}
			}, TAG);
			obsLogger.info(String.format("Node %s[%s] running status set to false", getNode().getName(), getNode().getId()));
			CommonUtils.handleAnyError(this::doClose, err -> obsLogger.warn(String.format("Close node failed: %s | Node: %s[%s] | Type: %s", err.getMessage(), getNode().getName(), getNode().getId(), this.getClass().getName())));
			CommonUtils.ignoreAnyError(() -> {
				if (this instanceof HazelcastProcessorBaseNode || this instanceof HazelcastMultiAggregatorProcessor) {
					AspectUtils.executeAspect(ProcessorNodeCloseAspect.class, () -> new ProcessorNodeCloseAspect().processorBaseContext(processorBaseContext));
				} else {
					AspectUtils.executeAspect(DataNodeCloseAspect.class, () -> new DataNodeCloseAspect().dataProcessorContext((DataProcessorContext) processorBaseContext));
				}
			}, TAG);
		} finally {
			ThreadContext.clearAll();
			super.close();
			sw.stop();
			obsLogger.info(String.format("Node %s[%s] close complete, cost %d ms", getNode().getName(), getNode().getId(), sw.getTotalTimeMillis()));
		}
	}

	protected void onDataStats(OnData onData, Stats stats) {
		Map<String, Long> total = stats.getTotal();
		Long processed = total.getOrDefault(Stats.PROCESSED_FIELD_NAME, 0L);
		Long sourceReceived = total.getOrDefault(Stats.SOURCE_RECEIVED_FIELD_NAME, 0L);
		Long targetInserted = total.getOrDefault(Stats.TARGET_INSERTED_FIELD_NAME, 0L);
		Long totalUpdated = total.getOrDefault(Stats.TOTAL_UPDATED_FIELD_NAME, 0L);
		Long totalDeleted = total.getOrDefault(Stats.TOTAL_DELETED_FIELD_NAME, 0L);
		Long fileSize = total.getOrDefault(Stats.TOTAL_FILE_LENGTH_FIELD_NAME, 0L);
		Long totalDataQuality = total.getOrDefault(Stats.TOTAL_DATA_QUAILTY_FIELD_NAME, 0L);

		processed += onData.getProcessed();
		sourceReceived += onData.getSource_received();
		targetInserted += onData.getTarget_inserted();
		totalUpdated += onData.getTotal_updated();
		totalDeleted += onData.getTotal_deleted();
		fileSize += onData.getTotal_file_length();
		if (onData.getTotal_data_quality() <= onData.getProcessed()) {
			totalDataQuality += onData.getTotal_data_quality();
		}
		totalDataQuality = totalDataQuality > processed ? processed : totalDataQuality;

		total.put(Stats.PROCESSED_FIELD_NAME, processed);
		total.put(Stats.SOURCE_RECEIVED_FIELD_NAME, sourceReceived);
		total.put(Stats.TARGET_INSERTED_FIELD_NAME, targetInserted);
		total.put(Stats.TOTAL_UPDATED_FIELD_NAME, totalUpdated);
		total.put(Stats.TOTAL_DELETED_FIELD_NAME, totalDeleted);
		total.put(Stats.TOTAL_FILE_LENGTH_FIELD_NAME, fileSize);
		total.put(Stats.TOTAL_DATA_QUAILTY_FIELD_NAME, totalDataQuality);

		List<StageRuntimeStats> stageRuntimeStats = stats.getStageRuntimeStats();

		for (StageRuntimeStats stageRuntimeStat : stageRuntimeStats) {
			String stageId = stageRuntimeStat.getStageId();
			if (onData.getInsertStage().containsKey(stageId)) {
				RuntimeThroughput runtimeThroughput = onData.getInsertStage().get(stageId);
				stageRuntimeStat.incrementInsert(runtimeThroughput);
			}
			if (onData.getUpdateStage().containsKey(stageId)) {
				RuntimeThroughput runtimeThroughput = onData.getUpdateStage().get(stageId);
				stageRuntimeStat.incrementUpdate(runtimeThroughput);
			}
			if (onData.getDeleteStage().containsKey(stageId)) {
				RuntimeThroughput runtimeThroughput = onData.getDeleteStage().get(stageId);
				stageRuntimeStat.incrementDelete(runtimeThroughput);
			}
		}
	}

	public Node<?> getNode() {
		return processorBaseContext.getNode();
	}

	public synchronized TapCodeException errorHandle(Throwable throwable) {
		return errorHandle(throwable, null);
	}

	public synchronized TapCodeException errorHandle(Throwable throwable, String errorMessage) {
		TapCodeException currentEx = wrapTapCodeException(throwable);
		TaskDto taskDto = processorBaseContext.getTaskDto();
		handleWhenTestRun(taskDto, currentEx);

		try {
			if (globalErrorIsNull()) {
				this.error = currentEx;
				getErrorMessage(errorMessage, currentEx);
				obsLogger.error(errorMessage, currentEx);
				this.running.set(false);

				// jetContext async injection, Attempt 5 times to get the instance every 500ms
				com.hazelcast.jet.Job hazelcastJob = getJetJob(taskDto);

				if (hazelcastJob != null) {
					JobStatus status = hazelcastJob.getStatus();
					stopJetJobIfStatusIsRunning(status, taskDto, hazelcastJob);
				} else {
					throw currentEx;
				}
			}
		} catch (NodeException e) {
			throw e;
		} catch (Exception e) {
			obsLogger.warn("Error handler failed: " + e.getMessage(), e);
		}

		return currentEx;
	}

	private void stopJetJobIfStatusIsRunning(JobStatus status, TaskDto taskDto, com.hazelcast.jet.Job hazelcastJob) {
		if (JobStatus.RUNNING == status) {
			obsLogger.info("Job suspend in error handle");
			TaskClient<TaskDto> taskDtoTaskClient = BeanUtil.getBean(TapdataTaskScheduler.class).getTaskClientMap().get(taskDto.getId().toHexString());
			if (null != taskDtoTaskClient) {
				taskDtoTaskClient.terminalMode(TerminalMode.ERROR);
				taskDtoTaskClient.error(error);
			}
			hazelcastJob.suspend();
		}
	}

	@Nullable
	protected com.hazelcast.jet.Job getJetJob(TaskDto taskDto) {
		com.hazelcast.jet.Job hazelcastJob = null;
		for (int i = 5; i > 0; i--) {
			if (null != jetContext) {
				hazelcastJob = jetContext.hazelcastInstance().getJet().getJob(taskDto.getName() + "-" + taskDto.getId().toHexString());
			}

			if (null != hazelcastJob) break;
			try {
				TimeUnit.MILLISECONDS.sleep(500L);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		return hazelcastJob;
	}

	private void getErrorMessage(String errorMessage, TapCodeException currentEx) {
		if (null != errorMessage) {
			this.errorMessage = errorMessage;
		} else {
			this.errorMessage = currentEx.getMessage();
		}
	}

	private boolean globalErrorIsNull() {
		return null == error;
	}

	private void handleWhenTestRun(TaskDto taskDto, TapCodeException currentEx) {
		if (StringUtils.equalsAnyIgnoreCase(processorBaseContext.getTaskDto().getSyncType(), TaskDto.SYNC_TYPE_TEST_RUN)) {
			TestRunTaskHandler.setError(taskDto.getId().toHexString(), currentEx);
		}
	}

	@NotNull
	protected static TapCodeException wrapTapCodeException(Throwable throwable) {
		TapCodeException currentEx;
		if (null == throwable) throw new IllegalArgumentException("Input exception cannot be null");
		Throwable matchThrowable = CommonUtils.matchThrowable(throwable, TapCodeException.class);
		if (null != matchThrowable) {
			currentEx = (TapCodeException) matchThrowable;
		} else {
			currentEx = new TapProcessorUnknownException(throwable);
		}
		return currentEx;
	}

	protected boolean taskHasBeenRun() {
		final TaskDto taskDto = processorBaseContext.getTaskDto();
		if (taskDto != null && MapUtils.isNotEmpty(taskDto.getAttrs())) {
			return taskDto.getAttrs().containsKey("syncProgress");
		}

		return false;
	}

	@Override
	public boolean isCooperative() {
		return false;
	}

	protected boolean isRunning() {
		//isJetJobRunning has thread lock and not a simple implementation.
		//Should avoid invoke isJetJobRunning method for every event.
		//Use TapCache to cache the isJetJobRunning's result, expire in 2 seconds.
		//Then no more performance issue.
		return running.get() && !Thread.currentThread().isInterrupted() && isJetJobRunning();
	}


	protected void updateMemoryFromDDLInfoMap(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) {
			return;
		}
		if (!tapdataEvent.isDDL()) {
			return;
		}
		try {
			updateDAG(tapdataEvent);
		} catch (Exception e) {
			throw new TapCodeException(TaskProcessorExCode_11.UPDATE_MEMORY_DAG_FAILED, e);
		}
		try {
			updateNode(tapdataEvent);
		} catch (Exception e) {
			throw new TapCodeException(TaskProcessorExCode_11.UPDATE_MEMORY_NODE_CONFIG_FAILED, e);
		}
		try {
			if (getNode() instanceof MergeTableNode) {
				updateTapTable(tapdataEvent, getNode().getId());
				updateTapTable(tapdataEvent, TapEventUtil.getTableId(tapdataEvent.getTapEvent()));
			} else {
				String tgtTableNameFromTapEvent = getTgtTableNameFromTapEvent(tapdataEvent.getTapEvent());
				updateTapTable(tapdataEvent, tgtTableNameFromTapEvent);
			}
		} catch (Exception e) {
			throw new TapCodeException(TaskProcessorExCode_11.UPDATE_MEMORY_TAP_TABLE_FAILED, e);
		}
	}

	protected void updateDAG(TapdataEvent tapdataEvent) {
		Object newDAG = tapdataEvent.getTapEvent().getInfo(NEW_DAG_INFO_KEY);
		if (!(newDAG instanceof DAG)) {
			return;
		}
		DAG dag = ((DAG) newDAG);
		processorBaseContext.getTaskDto().setDag(dag);
		processorBaseContext.setNodes(dag.getNodes());
		processorBaseContext.setEdges(dag.getEdges());
	}

	protected void updateNode(TapdataEvent tapdataEvent) {
		Object newDAG = tapdataEvent.getTapEvent().getInfo(NEW_DAG_INFO_KEY);
		if (!(newDAG instanceof DAG)) {
			return;
		}
		String nodeId = getNode().getId();
		Node<?> newNode = ((DAG) newDAG).getNode(nodeId);
		processorBaseContext.setNode(newNode);
		updateNodeConfig(tapdataEvent);
	}

	protected void updateTapTable(TapdataEvent tapdataEvent) {
		updateTapTable(tapdataEvent, null);
	}

	protected void updateTapTable(TapdataEvent tapdataEvent, String tableName) {
		Object dagDataService = tapdataEvent.getTapEvent().getInfo(DAG_DATA_SERVICE_INFO_KEY);
		if (!(dagDataService instanceof DAGDataServiceImpl)) {
			return;
		}
		TapEvent tapEvent = tapdataEvent.getTapEvent();
		TapTableMap<String, TapTable> tapTableMap = processorBaseContext.getTapTableMap();
		if (StringUtils.isBlank(tableName)) {
			tableName = getNode().getId();
		}
		String qualifiedName = tapTableMap.getQualifiedName(tableName);
		if (tapEvent instanceof TapCreateTableEvent) {
			updateTapTableWhenCreateTableEvent(tableName, tapEvent, (DAGDataServiceImpl) dagDataService, tapTableMap);
		} else if (tapEvent instanceof TapDropTableEvent) {
			// do nothing
		} else {
			updateTapTableWhenDDLEvent(tableName, qualifiedName, (DAGDataServiceImpl) dagDataService, tapTableMap, tapEvent);
		}
	}

	private static void updateTapTableWhenDDLEvent(String tableName, String qualifiedName, DAGDataServiceImpl dagDataService, TapTableMap<String, TapTable> tapTableMap, TapEvent tapEvent) {
		if (StringUtils.isBlank(qualifiedName)) {
			throw new TapCodeException(TaskProcessorExCode_11.UPDATE_TAP_TABLE_QUALIFIED_NAME_EMPTY, String.format("Table name: %s", tableName));
		}
		TapTable tapTable = dagDataService.getTapTable(qualifiedName);
		tapTableMap.put(tableName, tapTable);
		Object updateMetadataObj = tapEvent.getInfo(UPDATE_METADATA_INFO_KEY);
		if (updateMetadataObj instanceof Map) {
			MetadataInstancesDto metadata = dagDataService.getMetadata(qualifiedName);
			if (null == metadata.getId()) {
				Object qualifiedNameIdMap = tapEvent.getInfo(QUALIFIED_NAME_ID_MAP_INFO_KEY);
				if (qualifiedNameIdMap instanceof Map) {
					Object id = ((Map<?, ?>) qualifiedNameIdMap).get(qualifiedName);
					if (id instanceof String && StringUtils.isNotBlank((String) id)) {
						metadata.setId(new ObjectId((String) id));
					}
				}
				if (null == metadata.getId()) {
					throw new TapCodeException(TaskProcessorExCode_11.TRANSFORM_METADATA_ID_NULL);
				}
			}
			((Map<String, MetadataInstancesDto>) updateMetadataObj).put(metadata.getId().toHexString(), metadata);
		}
	}

	private void updateTapTableWhenCreateTableEvent(String tableName, TapEvent tapEvent, DAGDataServiceImpl dagDataService, TapTableMap<String, TapTable> tapTableMap) {
		String qualifiedName;
		Object insertMetadata = tapEvent.getInfo(INSERT_METADATA_INFO_KEY);
		if (insertMetadata instanceof List) {
			MetadataInstancesDto metadata = dagDataService.getSchemaByNodeAndTableName(getNode().getId(), tableName);
			if (null != metadata) {
				qualifiedName = metadata.getQualifiedName();
				if (null == metadata.getId()) {
					metadata.setId(new ObjectId());
				}
				dagDataService.setMetaDataMap(metadata);
				((List<MetadataInstancesDto>) insertMetadata).add(metadata);
				TapTable tapTable = dagDataService.getTapTable(qualifiedName);
				if (tapTableMap.containsKey(tableName)) {
					tapTableMap.put(tableName, tapTable);
				} else {
					tapTableMap.putNew(tableName, tapTable, qualifiedName);
				}
			} else {
				throw new TapCodeException(TaskProcessorExCode_11.GET_NODE_METADATA_BY_TABLE_NAME_FAILED, String.format("Node: %s(%s), table name: %s", getNode().getName(), getNode().getId(), tableName));
			}
		}
	}

	protected void updateNodeConfig(TapdataEvent tapdataEvent) {
	}

	protected String getTgtTableNameFromTapEvent(TapEvent tapEvent) {
		String tableId = TapEventUtil.getTableId(tapEvent);
		Object dagDataService = tapEvent.getInfo(DAG_DATA_SERVICE_INFO_KEY);
		if (!(dagDataService instanceof DAGDataServiceImpl)) {
			return StringUtils.isNotBlank(lastTableName) ? lastTableName : tableId;
		}
		String nodeId = getNode().getId();
		return ((DAGDataServiceImpl) dagDataService).getNameByNodeAndTableName(nodeId, tableId);
	}

	public static class TapValueTransform {
		private Map<String, TapValue<?, ?>> before;
		private Map<String, TapValue<?, ?>> after;

		public static TapValueTransform create() {
			return new TapValueTransform();
		}

		public TapValueTransform before(Map<String, TapValue<?, ?>> before) {
			this.before = before;
			return this;
		}

		public TapValueTransform after(Map<String, TapValue<?, ?>> after) {
			this.after = after;
			return this;
		}

		public Map<String, TapValue<?, ?>> getBefore() {
			return before;
		}

		public Map<String, TapValue<?, ?>> getAfter() {
			return after;
		}
	}

	@Nullable
	protected JobStatus getJetJobStatus() {
		if (null == jetJobStatusMonitor) {
			return null;
		}
		return jetJobStatusMonitor.get();
	}

	protected boolean isJetJobRunning() {
		JobStatus jetJobStatus = getJetJobStatus();
		return null == jetJobStatus || jetJobStatus.equals(JobStatus.RUNNING);
	}

	public ObsLogger getObsLogger() {
		return obsLogger;
	}
}
