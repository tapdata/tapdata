package io.tapdata.observable.logging;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.ClearTableFuncAspect;
import io.tapdata.aspect.CreateIndexFuncAspect;
import io.tapdata.aspect.CreateTableFuncAspect;
import io.tapdata.aspect.DataNodeInitAspect;
import io.tapdata.aspect.DropTableFuncAspect;
import io.tapdata.aspect.ProcessorFunctionAspect;
import io.tapdata.aspect.ProcessorNodeInitAspect;
import io.tapdata.aspect.ProcessorNodeProcessAspect;
import io.tapdata.aspect.SourceStateAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.TableCountFuncAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.WriteRecordFuncAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.observable.logging.tag.LogTag;
import io.tapdata.observable.logging.tag.SourceNodeTag;
import io.tapdata.observable.logging.tag.TargetNodeTag;
import io.tapdata.observable.metric.aspect.ConnectionPingAspect;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC})
public class LoggingAspectTask extends AspectTask {
	private final ClassHandlers loggingClassHandlers = new ClassHandlers();
	private ObsLogger taskLogger;
	private final ConcurrentHashMap<String, ObsLogger> nodeLoggerMap = new ConcurrentHashMap<>();

	public LoggingAspectTask() {
		loggingClassHandlers.register(DataNodeInitAspect.class, this::handleDataNodeInit);
		loggingClassHandlers.register(ProcessorNodeInitAspect.class, this::handleProcessorNodeInit);

		loggingClassHandlers.register(ConnectionPingAspect.class, this::handleConnectionPing);

		loggingClassHandlers.register(SourceStateAspect.class, this::handleSourceState);
		loggingClassHandlers.register(TableCountFuncAspect.class, this::handleTableCount);
		loggingClassHandlers.register(BatchReadFuncAspect.class, this::handleBatchReadFunc);
		loggingClassHandlers.register(StreamReadFuncAspect.class, this::handleStreamReadFunc);

		loggingClassHandlers.register(DropTableFuncAspect.class, this::handleDropTableFuc);
		loggingClassHandlers.register(ClearTableFuncAspect.class, this::handleClearTableFuc);
		loggingClassHandlers.register(CreateTableFuncAspect.class, this::handleCreateTableFuc);
		loggingClassHandlers.register(CreateIndexFuncAspect.class, this::handleCreateIndexFuc);
		loggingClassHandlers.register(WriteRecordFuncAspect.class, this::handleWriteRecordFunc);

		loggingClassHandlers.register(ProcessorNodeProcessAspect.class, this::handleProcessorNodeProcessFunc);
	}

	/**
	 * The task started
	 */
	@Override
	public void onStart(TaskStartAspect startAspect) {
		this.taskLogger = ObsLoggerFactory.getInstance().getObsLogger(task);
		getObsLogger().info("Task initialization...");
	}

	/**
	 * The task stopped
	 */
	@Override
	public void onStop(TaskStopAspect stopAspect) {
//		if (null != stopAspect.getError()) {
//			handleTaskErrorStop(stopAspect);
//		}

		// finally remove the logger
		ObsLoggerFactory.getInstance().removeTaskLoggerMarkRemove(task);
	}

	public void handleTaskErrorStop(TaskStopAspect aspect) {
		Throwable throwable = aspect.getError();
		if (null == throwable) {
			return;
		}

		List<TapEvent> events = null;
		ProcessorBaseContext context = null;
		if (throwable.getClass().getSimpleName().equals("NodeException")) {
			try {
				Method getEvents = throwable.getClass().getMethod("getEvents");
				Object eventsObj = getEvents.invoke(throwable);
				if (null != eventsObj) {
					events = new ArrayList<>((List<TapEvent>) eventsObj);
				}

				Method getContext = throwable.getClass().getMethod("getContext");
				Object contextObj = getContext.invoke(throwable);

				if (null != contextObj) {
					context = (ProcessorBaseContext) contextObj;
				}
			} catch (Throwable ignore) {}
		}

		error(throwable, context, events);
	}


	private ObsLogger getObsLogger() {
		return taskLogger;
	}

	private ObsLogger getObsLogger(Node<?> node) {
		return nodeLoggerMap.computeIfAbsent(
				node.getId(),
				nodeId -> ObsLoggerFactory.getInstance().getObsLogger(task, nodeId, node.getName())
		);
	}

	public boolean noNeedLog(String level) {
		return ((TaskLogger) getObsLogger()).noNeedLog(level);
	}

	private Collection<String> getPkFields(ProcessorBaseContext context, String tableName) {
		if (!(context instanceof DataProcessorContext)) {
			return Collections.emptyList();
		}

		TapTable table = null;
		try {
			table = context.getTapTableMap().get(tableName);
		} catch (Throwable ignored) {}
		if (null == table) {
			try {
				table = context.getTapTableMap().get(context.getNode().getId());
			} catch (Throwable ignored) {
			}
		}
		if (null == table) {
			return Collections.emptyList();
		}

		Collection<String> pkFields =  table.primaryKeys();
		if (null == pkFields || pkFields.isEmpty()) {
			pkFields = table.primaryKeys(true);
		}

		if (null == pkFields || pkFields.isEmpty()) {
			pkFields = table.getNameFieldMap().keySet();
		}

		return pkFields;
	}

	private void error(Throwable throwable) {
		if (noNeedLog(LogLevel.ERROR.getLevel())) {
			return;
		}
		ObsLogger obsLogger = getObsLogger();
		obsLogger.error(obsLogger::logBaseBuilder, throwable);
	}

	private void error(Throwable throwable, ProcessorBaseContext context) {
		if (noNeedLog(LogLevel.ERROR.getLevel())) {
			return;
		}

		if (null == context) {
			error(throwable);
			return;
		}

		Node<?> node = context.getNode();
		if (null == node) {
			error(throwable);
			return;
		}

		LogEventData.LogEventDataBuilder builder = LogEventData.builder()
				.eventType(LogEventData.LOG_EVENT_TYPE_PROCESS)
				.status(LogEventData.LOG_EVENT_STATUS_ERROR)
				.message(throwable.getMessage())
				.time(System.currentTimeMillis())
				.withNode(node);

		ObsLogger obsLogger = getObsLogger(node);
		obsLogger.error(() -> obsLogger.logBaseBuilder().record(builder.build().toMap()), throwable);
	}

	private void error(Throwable throwable, ProcessorBaseContext context, List<TapEvent> events) {
		if (noNeedLog(LogLevel.ERROR.getLevel())) {
			return;
		}

		if (null == events || events.isEmpty()) {
			error(throwable, context);
			return;
		}

		Node<?> node = context.getNode();
		List<Map<String, Object>> data = new ArrayList<>();
		for(TapEvent event : events) {
			if (null == event) {
				continue;
			}
			TapBaseEvent baseEvent = (TapBaseEvent) event;
			Collection<String> pkFields = getPkFields(context, baseEvent.getTableId());
			data.add(LogEventData.builder()
					.eventType(LogEventData.LOG_EVENT_TYPE_PROCESS)
					.status(LogEventData.LOG_EVENT_STATUS_ERROR)
					.message(throwable.getMessage())
					.time(System.currentTimeMillis())
					.withNode(node)
					.withTapEvent(event, pkFields)
					.build().toMap());
		}

		ObsLogger obsLogger = null != node ? getObsLogger(node) : getObsLogger();
		obsLogger.error(() -> obsLogger.logBaseBuilder().data(data), throwable);
	}

	private void debug(String logEventType, Long cost, LogTag tag, ProcessorBaseContext context) {
		if (noNeedLog(LogLevel.DEBUG.getLevel())) {
			return;
		}

		Node<?> node = context.getNode();
		if (null == node) {
			return;
		}

		LogEventData.LogEventDataBuilder builder = LogEventData.builder()
				.eventType(logEventType)
				.status(LogEventData.LOG_EVENT_STATUS_OK)
				.time(System.currentTimeMillis())
				.cost(cost)
				.withNode(node);

		ObsLogger obsLogger = getObsLogger(node);
		obsLogger.debug(() -> obsLogger.logBaseBuilderWithLogTag(tag).record(builder.build().toMap()), builder.build().getMessage());
	}


	private void debug(String logEventType, Long cost, LogTag tag, ProcessorBaseContext context, List<? extends TapEvent> events) {
		if (noNeedLog(LogLevel.DEBUG.getLevel())) {
			return;
		}

		if (null == events || events.isEmpty()) {
			debug(logEventType, cost, tag, context);
			return;
		}

		Node<?> node = context.getNode();
		List<Map<String, Object>> data = new ArrayList<>();
		for (TapEvent event : events) {
			if (null == event) {
				continue;
			}

			LogEventData.LogEventDataBuilder logEventDataBuilder = LogEventData.builder()
					.eventType(logEventType)
					.status(LogEventData.LOG_EVENT_STATUS_OK)
					.time(System.currentTimeMillis())
					.cost(cost)
					.withNode(node);

			if (event instanceof TapBaseEvent) {
				TapBaseEvent baseEvent = (TapBaseEvent) event;
				Collection<String> pkFields = getPkFields(context, baseEvent.getTableId());
				logEventDataBuilder.withTapEvent(baseEvent, pkFields);
			}
			data.add(logEventDataBuilder.build().toMap());
		}

		ObsLogger obsLogger = getObsLogger(node);
		obsLogger.debug(() -> obsLogger.logBaseBuilderWithLogTag(tag).data(data), logEventType);
	}

	private void debug(String logEventType, Long cost, LogTag tag, ProcessorBaseContext context, TapdataEvent event) {
		if (noNeedLog(LogLevel.DEBUG.getLevel())) {
			return;
		}

		if (null == event || null == event.getTapEvent()) {
			debug(logEventType, cost, tag, context);
			return;
		}

		Node<?> node = context.getNode();
		List<Map<String, Object>> data = new ArrayList<>();
		LogEventData.LogEventDataBuilder logEventDataBuilder = LogEventData.builder()
				.eventType(logEventType)
				.status(LogEventData.LOG_EVENT_STATUS_OK)
				.time(System.currentTimeMillis())
				.cost(cost)
				.withNode(node);
		TapEvent tapEvent = event.getTapEvent();
		if (tapEvent instanceof TapBaseEvent) {
			TapBaseEvent baseEvent = (TapBaseEvent) event.getTapEvent();
			if (null == baseEvent) {
				return;
			}
			Collection<String> pkFields = getPkFields(context, baseEvent.getTableId());
			logEventDataBuilder.withTapEvent(baseEvent, pkFields);
		}
		data.add(logEventDataBuilder.build().toMap());

		ObsLogger obsLogger = getObsLogger(node);
		obsLogger.debug(() -> obsLogger.logBaseBuilderWithLogTag(tag).data(data), logEventType);
	}

	private void debug(String logEventType, Long cost, LogTag tag, ProcessorBaseContext context, TapEvent event) {
		debug(logEventType, cost, tag, context, Collections.singletonList(event));
	}

	/**
	 *  Init the TaskLoggerNodeProxy
	 * @param aspect
	 * @return
	 */
	public Void handleDataNodeInit(DataNodeInitAspect aspect) {
		getObsLogger(aspect.getDataProcessorContext().getNode());

		return null;
	}

	/**
	 *  Init the TaskLoggerNodeProxy
	 * @param aspect
	 * @return
	 */
	public Void handleProcessorNodeInit(ProcessorNodeInitAspect aspect) {
		getObsLogger(aspect.getProcessorBaseContext().getNode());

		return null;
	}

	public Void handleSourceState(SourceStateAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();

		switch (aspect.getState()) {
			case SourceStateAspect.STATE_INITIAL_SYNC_START:
				getObsLogger(node).info("Initial sync started");
				break;
			case SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED:
				getObsLogger(node).info("Initial sync completed");
				break;
			case SourceStateAspect.STATE_CDC_START:
				getObsLogger(node).info("Incremental sync starting...");
				break;
			case SourceStateAspect.STATE_CDC_COMPLETED:
				getObsLogger(node).info("Incremental sync completed");
				break;
			default:
				break;
		}

		return null;
	}

	Map<String, Map<String, Long>> tableCountMap = new HashMap<>();
	public Void handleTableCount(TableCountFuncAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();

		switch (aspect.getState()) {
			case TableCountFuncAspect.STATE_START:
				aspect.tableCountConsumer((table, cnt) -> {
					tableCountMap.putIfAbsent(node.getId(), new HashMap<>());
					tableCountMap.get(node.getId()).put(table, cnt);
				});
				break;
			case TableCountFuncAspect.STATE_END:
				break;
		}
		return null;
	}

	private final Map<String, Long> batchReadCompleteLastTs = new HashMap<>();
	private final Map<String, Long> batchEnqueuedLastTs = new HashMap<>();
	public Void handleBatchReadFunc(BatchReadFuncAspect aspect) {
		ProcessorBaseContext context = aspect.getDataProcessorContext();
		Node<?> node = context.getNode();
		String nodeId = node.getId();

		switch (aspect.getState()) {
			case BatchReadFuncAspect.STATE_START:
				batchReadCompleteLastTs.put(nodeId, aspect.getTime());
				Long total = tableCountMap.isEmpty() ? 0L : tableCountMap.get(nodeId).get(aspect.getTable().getName());
				getObsLogger(node).info("Table {} is going to be initial synced, sync size: {}",
						aspect.getTable().getName(), total);
				aspect.readCompleteConsumer(events -> {
					long now = System.currentTimeMillis();
					debug(LogEventData.LOG_EVENT_TYPE_RECEIVE, now - batchReadCompleteLastTs.get(nodeId),
							SourceNodeTag.NODE_SOURCE_INITIAL_SYNC, context, events);
					batchReadCompleteLastTs.put(nodeId, System.currentTimeMillis());
				});

				aspect.enqueuedConsumer(events -> {
					long now = System.currentTimeMillis();
					if  (null == batchEnqueuedLastTs.get(nodeId)) {
						batchEnqueuedLastTs.put(nodeId, now);
					}
					debug(LogEventData.LOG_EVENT_TYPE_SEND, now - batchEnqueuedLastTs.get(nodeId),
							SourceNodeTag.NODE_SOURCE_INITIAL_SYNC, context,
							events.stream().map(TapdataEvent::getTapEvent).collect(Collectors.toList()));
				});
				break;
			default:
		}

		return null;
	}

	private final Map<String, Long> streamReadCompleteLastTs = new HashMap<>();
	private final Map<String, Long> streamEnqueuedLastTs = new HashMap<>();
	public Void handleStreamReadFunc(StreamReadFuncAspect aspect) {
		ProcessorBaseContext context = aspect.getDataProcessorContext();
		Node<?> node = context.getNode();
		String nodeId = node.getId();

		switch (aspect.getState()) {
			case BatchReadFuncAspect.STATE_START:
				streamReadCompleteLastTs.put(nodeId, System.currentTimeMillis());
				aspect.streamingReadCompleteConsumers(events -> {
					Long now = System.currentTimeMillis();
					debug(LogEventData.LOG_EVENT_TYPE_RECEIVE, now - streamReadCompleteLastTs.get(nodeId),
							SourceNodeTag.NODE_SOURCE_INCREMENTAL_SYNC, context, events);
				});
				aspect.streamingEnqueuedConsumers(events -> {
					long now = System.currentTimeMillis();
					streamEnqueuedLastTs.putIfAbsent(nodeId, now);
					debug(LogEventData.LOG_EVENT_TYPE_SEND, now - streamEnqueuedLastTs.get(nodeId),
							SourceNodeTag.NODE_SOURCE_INCREMENTAL_SYNC, context,
							events.stream().map(TapdataEvent::getTapEvent).collect(Collectors.toList()));
				});
				break;
			default:
		}

		return null;
	}

	public Void handleDropTableFuc(DropTableFuncAspect aspect) {
		ProcessorBaseContext context = aspect.getDataProcessorContext();
		Node<?> node = context.getNode();

		switch (aspect.getState()) {
			case DropTableFuncAspect.STATE_START:
				debug(LogEventData.LOG_EVENT_TYPE_RECEIVE, null, TargetNodeTag.NODE_TARGET_CREATE_TABLE, context,
						aspect.getDropTableEvent());
				debug(LogEventData.LOG_EVENT_TYPE_PROCESS, null, TargetNodeTag.NODE_TARGET_CREATE_TABLE, context,
						aspect.getDropTableEvent());
				break;
			case DropTableFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	public Void handleClearTableFuc(ClearTableFuncAspect aspect) {
		ProcessorBaseContext context = aspect.getDataProcessorContext();
		Node<?> node = context.getNode();

		switch (aspect.getState()) {
			case ClearTableFuncAspect.STATE_START:
				debug(LogEventData.LOG_EVENT_TYPE_RECEIVE, null, TargetNodeTag.NODE_TARGET_CREATE_TABLE, context,
						aspect.getClearTableEvent());
				debug(LogEventData.LOG_EVENT_TYPE_PROCESS, null, TargetNodeTag.NODE_TARGET_CREATE_TABLE, context,
						aspect.getClearTableEvent());
				break;
			case ClearTableFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	public Void handleCreateTableFuc(CreateTableFuncAspect aspect) {
		ProcessorBaseContext context = aspect.getDataProcessorContext();
		Node<?> node = context.getNode();

		switch (aspect.getState()) {
			case CreateTableFuncAspect.STATE_START:
				debug(LogEventData.LOG_EVENT_TYPE_RECEIVE, null, TargetNodeTag.NODE_TARGET_CREATE_TABLE, context,
						aspect.getCreateTableEvent());
				debug(LogEventData.LOG_EVENT_TYPE_PROCESS, null, TargetNodeTag.NODE_TARGET_CREATE_TABLE, context,
						aspect.getCreateTableEvent());
				break;
			case CreateTableFuncAspect.STATE_END:
				if (null != aspect.getCreateTableOptions() && aspect.getCreateTableOptions().getTableExists()) {
					getObsLogger(node).info("The table {} has already exist.", aspect.getCreateTableEvent().getTable().getName());
				}
				break;
		}

		return null;
	}

	public Void handleCreateIndexFuc(CreateIndexFuncAspect aspect) {
		ProcessorBaseContext context = aspect.getDataProcessorContext();
		Node<?> node = context.getNode();

		switch (aspect.getState()) {
			case CreateTableFuncAspect.STATE_START:
				debug(LogEventData.LOG_EVENT_TYPE_RECEIVE, null, TargetNodeTag.NODE_TARGET_CREATE_INDEX, context,
						aspect.getCreateIndexEvent());
				debug(LogEventData.LOG_EVENT_TYPE_PROCESS, null, TargetNodeTag.NODE_TARGET_CREATE_INDEX, context,
						aspect.getCreateIndexEvent());
				break;
			case CreateTableFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	private final Map<String, Long> writeRecordAcceptLastTs = new HashMap<>();
	public Void handleWriteRecordFunc(WriteRecordFuncAspect aspect) {
		ProcessorBaseContext context = aspect.getDataProcessorContext();
		Node<?> node = context.getNode();
		String nodeId = node.getId();

		switch (aspect.getState()) {
			case WriteRecordFuncAspect.STATE_START:
				writeRecordAcceptLastTs.put(nodeId, System.currentTimeMillis());
				debug(LogEventData.LOG_EVENT_TYPE_RECEIVE, null, null, context, aspect.getRecordEvents());
				aspect.consumer((events, result) -> {
					long now = System.currentTimeMillis();
					debug(LogEventData.LOG_EVENT_TYPE_SEND, now - writeRecordAcceptLastTs.get(nodeId),
							null, context, events);
				});
				break;
			case WriteRecordFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	public Void handleConnectionPing(ConnectionPingAspect aspect) {
		return null;
	}

	public Void handleProcessorNodeProcessFunc(ProcessorNodeProcessAspect aspect) {
		ProcessorBaseContext context = aspect.getProcessorBaseContext();
		Node<?> node = context.getNode();

		switch (aspect.getState()) {
			case ProcessorNodeProcessAspect.STATE_START:
				aspect.consumer(event -> {
					debug(LogEventData.LOG_EVENT_TYPE_RECEIVE, 0L, null, context, event);
				});
				break;
			case ProcessorFunctionAspect.STATE_END:
				debug(LogEventData.LOG_EVENT_TYPE_SEND, aspect.getTime(), null, context, aspect.getInputEvent());
				break;
			default:
		}

		return null;
	}

	@Override
	public List<Class<? extends Aspect>> observeAspects() {
		List<Class<? extends Aspect>> aspects = new ArrayList<>();
		for (Class<?> clazz : loggingClassHandlers.keyList()) {
			aspects.add((Class<? extends Aspect>) clazz);
		}

		return aspects;
	}

	@Override
	public List<Class<? extends Aspect>> interceptAspects() {
		return null;
	}

	@Override
	public void onObserveAspect(Aspect aspect) {
		loggingClassHandlers.handle(aspect);
	}

	@Override
	public AspectInterceptResult onInterceptAspect(Aspect aspect) {
		return null;
	}
}
