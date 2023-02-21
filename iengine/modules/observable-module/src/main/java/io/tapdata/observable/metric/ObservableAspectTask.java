package io.tapdata.observable.metric;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.module.api.PipelineDelay;
import io.tapdata.observable.metric.handler.*;

import java.util.*;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC})
public class ObservableAspectTask extends AspectTask {
	private final ClassHandlers observerClassHandlers = new ClassHandlers();

	private TaskSampleHandler taskSampleHandler;
	private Map<String, TableSampleHandler> tableSampleHandlers;
	private Map<String, DataNodeSampleHandler> dataNodeSampleHandlers;
	private Map<String, ProcessorNodeSampleHandler> processorNodeSampleHandlers;

	public ObservableAspectTask() {
		// data node aspects
		observerClassHandlers.register(DataNodeInitAspect.class, this::handleDataNodeInit);
		observerClassHandlers.register(PDKNodeInitAspect.class, this::handlePDKNodeInit);
		observerClassHandlers.register(DataNodeCloseAspect.class, this::handleDataNodeClose);
		// source data node aspects
		observerClassHandlers.register(TableCountFuncAspect.class, this::handleTableCount);
		observerClassHandlers.register(BatchReadFuncAspect.class, this::handleBatchReadFunc);
		observerClassHandlers.register(StreamReadFuncAspect.class, this::handleStreamReadFunc);
		observerClassHandlers.register(SourceStateAspect.class, this::handleSourceState);
		observerClassHandlers.register(SourceDynamicTableAspect.class, this::handleSourceDynamicTable);
		// target data node aspects
		observerClassHandlers.register(WriteRecordFuncAspect.class, this::handleWriteRecordFunc);
		observerClassHandlers.register(NewFieldFuncAspect.class, this::handleNewFieldFun);
		observerClassHandlers.register(AlterFieldNameFuncAspect.class, this::handleAlterFieldNameFunc);
		observerClassHandlers.register(AlterFieldAttributesFuncAspect.class, this::handleAlterFieldAttributesFunc);
		observerClassHandlers.register(DropFieldFuncAspect.class, this::handleDropFieldFunc);
		observerClassHandlers.register(CreateTableFuncAspect.class, this::handleCreateTableFunc);
		observerClassHandlers.register(DropTableFuncAspect.class, this::handleDropTableFunc);

		// processor node aspects
		observerClassHandlers.register(ProcessorNodeInitAspect.class, this::handleProcessorNodeInit);
		observerClassHandlers.register(ProcessorNodeCloseAspect.class, this::handleProcessorNodeClose);
		observerClassHandlers.register(ProcessorNodeProcessAspect.class, this::handleProcessorNodeProcess);
	}


	/**
	 * The task started
	 */
	@Override
	public void onStart(TaskStartAspect startAspect) {
		taskSampleHandler = new TaskSampleHandler(task);
		taskSampleHandler.init();
	}

	/**
	 * The task stopped
	 */
	@Override
	public void onStop(TaskStopAspect stopAspect) {
		pipelineDelay.clear(stopAspect.getTask().getId().toHexString());
		if (null != tableSampleHandlers) {
			for (TableSampleHandler handler : tableSampleHandlers.values()) {
				handler.close();
			}
		}
		taskSampleHandler.close();
	}

	// data node related

	public Void handleDataNodeInit(DataNodeInitAspect aspect) {
		if (null == dataNodeSampleHandlers) {
			dataNodeSampleHandlers = new HashMap<>();
		}
		Node<?> node = aspect.getDataProcessorContext().getNode();
		DataNodeSampleHandler handler = new DataNodeSampleHandler(task, node);
		dataNodeSampleHandlers.put(node.getId(), handler);
		handler.init();

		return null;
	}

	public Void handleDataNodeClose(DataNodeCloseAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::close);
//		DataNodeSampleHandler.HealthCheckRunner.getInstance().stopHealthCheck(nodeId);

		return null;
	}

	public Void handlePDKNodeInit(PDKNodeInitAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		DataNodeSampleHandler handler = dataNodeSampleHandlers.get(node.getId());
//		if (null != handler) {
//			DataNodeSampleHandler.HealthCheckRunner.getInstance().runHealthCheck(
//					handler.getCollector(), node,  aspect.getDataProcessorContext().getPdkAssociateId());
//		}

		return null;
	}

	// source data node related

	Map<String, Map<String, Number>> taskRetrievedTableValues;
	public Void handleTableCount(TableCountFuncAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		switch (aspect.getState()) {
			case TableCountFuncAspect.STATE_START:
				if (null == taskRetrievedTableValues) {
					taskRetrievedTableValues = TableSampleHandler.retrieveAllTables(task);
				}
				for (String table : aspect.getDataProcessorContext().getTapTableMap().keySet()) {
					Optional.ofNullable(dataNodeSampleHandlers.get(node.getId())).ifPresent(
							dataNodeSampleHandler -> dataNodeSampleHandler.addTable(table)
					);
					taskSampleHandler.addTable(table);
				}
				aspect.tableCountConsumer((table, cnt) -> {
					if (null == tableSampleHandlers) {
						tableSampleHandlers = new HashMap<>();
					}
					TableSampleHandler handler = new TableSampleHandler(task, table, cnt, taskRetrievedTableValues.getOrDefault(table, new HashMap<>()));
					tableSampleHandlers.put(table, handler);
					handler.init();

					Optional.ofNullable(dataNodeSampleHandlers.get(node.getId())).ifPresent(
							dataNodeSampleHandler -> dataNodeSampleHandler.handleTableCountAccept(table, cnt)
					);
					taskSampleHandler.handleTableCountAccept(table, cnt);
				});
				break;
			case TableCountFuncAspect.STATE_END:
				break;
		}
		return null;
	}

	public Void handleBatchReadFunc(BatchReadFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		String table = aspect.getTable().getName();

		switch (aspect.getState()) {
			case BatchReadFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler -> handler.handleBatchReadFuncStart(table, aspect.getTime()));
				taskSampleHandler.addTable(table);
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
						dataNodeSampleHandler -> dataNodeSampleHandler.addTable(table)
				);
				taskSampleHandler.handleBatchReadStart(table);
				aspect.readCompleteConsumer(events -> {
					if (null == events || events.size() == 0) {
						return;
					}

					int size = events.size();
					Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler ->
							handler.handleBatchReadReadComplete(System.currentTimeMillis(), size));
					taskSampleHandler.handleBatchReadAccept(size);
				});
				aspect.processCompleteConsumer(events -> {
					if (null == events || events.isEmpty()) {
						return;
					}

					HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapdataEvent(events);
					Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler ->
							handler.handleBatchReadProcessComplete(System.currentTimeMillis(), recorder)
					);
					// batch read should calculate table snapshot insert counter

					Optional.ofNullable(tableSampleHandlers).flatMap(handlers -> Optional.ofNullable(handlers.get(table))).ifPresent(handler -> handler.incrTableSnapshotInsertTotal(recorder.getInsertTotal()));
				});
				aspect.enqueuedConsumer(events ->
					Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
							handler -> handler.handleBatchReadEnqueued(System.currentTimeMillis())
				));
				break;
			case BatchReadFuncAspect.STATE_END:
				if (!aspect.getDataProcessorContext().getTaskDto().isSnapShotInterrupt()) {
					Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(handler -> handler.handleBatchReadFuncEnd(System.currentTimeMillis()));
					taskSampleHandler.handleBatchReadFuncEnd();
					break;
				}
				break;
		}

		return null;
	}

	public Void handleStreamReadFunc(StreamReadFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();

		switch (aspect.getState()) {
			case StreamReadFuncAspect.STATE_START:
				List<String> tables = aspect.getTables();
				taskSampleHandler.handleStreamReadStart(tables);
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
						handler -> handler.handleStreamReadStreamStart(tables, aspect.getStreamStartedTime())
				);

				aspect.streamingReadCompleteConsumers(events -> {
					if (null == events || events.size() == 0) {
						return;
					}

					HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(events);
					Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
							handler -> {
								handler.handleStreamReadReadComplete(System.currentTimeMillis(), recorder);
							}
					);
					taskSampleHandler.handleStreamReadAccept(recorder);
				});

				aspect.streamingProcessCompleteConsumers(events -> {
					if (null == events || events.size() == 0) {
						return;
					}

					HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapdataEvent(events);

					Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
							handler -> {
								handler.handleStreamReadProcessComplete(System.currentTimeMillis(), recorder);
								taskSampleHandler.addTargetNodeHandler(nodeId, handler);
							}
					);
				});
				aspect.streamingEnqueuedConsumers(events -> {
					Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
							handler -> handler.handleStreamReadEnqueued(System.currentTimeMillis())
					);
				});
				break;
			case StreamReadFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	public Void handleSourceState(SourceStateAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		switch (aspect.getState()) {
			case SourceStateAspect.STATE_INITIAL_SYNC_START:
				taskSampleHandler.handleSnapshotStart(aspect.getInitialSyncStartTime());
				for(String table : aspect.getDataProcessorContext().getTapTableMap().keySet()) {
					taskSampleHandler.addTable(table);
				}

				Optional.ofNullable(dataNodeSampleHandlers.get(node.getId())).ifPresent(
						handler -> taskSampleHandler.addSourceNodeHandler(node.getId(), handler)
				);
				break;
			case SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED:
//				taskSampleHandler.handleSnapshotDone(aspect.getInitialSyncCompletedTime());
				break;
			default:
				break;
		}

		return null;
	}

	public Void handleSourceDynamicTable(SourceDynamicTableAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		switch (aspect.getType()) {
			case SourceDynamicTableAspect.DYNAMIC_TABLE_TYPE_ADD:
				taskSampleHandler.handleSourceDynamicTableAdd(aspect.getTables());
				Optional.ofNullable(dataNodeSampleHandlers.get(node.getId())).ifPresent(
						handler -> handler.handleSourceDynamicTableAdd(aspect.getTables())
				);
				break;
			case SourceDynamicTableAspect.DYNAMIC_TABLE_TYPE_REMOVE:
				taskSampleHandler.handleSourceDynamicTableRemove(aspect.getTables());
				Optional.ofNullable(dataNodeSampleHandlers.get(node.getId())).ifPresent(
						handler -> handler.handleSourceDynamicTableRemove(aspect.getTables())
				);
				break;
			default:
				break;
		}

		return null;
	}

	// target data node related

	public Void handleCreateTableFunc(CreateTableFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case CreateTableFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case CreateTableFuncAspect.STATE_END:
				taskSampleHandler.handleCreateTableEnd();
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleDropTableFunc(DropTableFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case DropTableFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case DropTableFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleNewFieldFun(NewFieldFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case NewFieldFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case NewFieldFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleAlterFieldNameFunc(AlterFieldNameFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case AlterFieldNameFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case AlterFieldNameFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleAlterFieldAttributesFunc(AlterFieldAttributesFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case AlterFieldAttributesFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case AlterFieldAttributesFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	public Void handleDropFieldFunc(DropFieldFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		switch (aspect.getState()) {
			case DropFieldFuncAspect.STATE_START:
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlStart);
				break;
			case DropFieldFuncAspect.STATE_END:
				taskSampleHandler.handleDdlEnd();
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(DataNodeSampleHandler::handleDdlEnd);
				break;
		}

		return null;
	}

	private PipelineDelayImpl pipelineDelay = (PipelineDelayImpl) InstanceFactory.instance(PipelineDelay.class);
	public Void handleWriteRecordFunc(WriteRecordFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();

		switch (aspect.getState()) {
			case WriteRecordFuncAspect.STATE_START:
				HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(aspect.getRecordEvents());
				Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
						handler -> {
							handler.handleWriteRecordStart(aspect.getTime(), recorder);
							taskSampleHandler.addTargetNodeHandler(nodeId, handler);
						}
				);
				aspect.consumer((events, result) -> {
					if (null == events || events.size() == 0) {
						return;
					}

					HandlerUtil.EventTypeRecorder inner = HandlerUtil.countTapEvent(events);
					Optional.ofNullable(dataNodeSampleHandlers.get(nodeId)).ifPresent(
							handler -> {
								handler.handleWriteRecordAccept(System.currentTimeMillis(), result, inner);
							}
					);
					taskSampleHandler.handleWriteRecordAccept(result, events);
					pipelineDelay.refreshDelay(task.getId().toHexString(), nodeId, inner.getProcessTimeTotal() / inner.getTotal(), inner.getNewestEventTimestamp());
				});
				break;
			case WriteRecordFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	// processor node related

	public Void handleProcessorNodeInit(ProcessorNodeInitAspect aspect) {
		if (null == processorNodeSampleHandlers) {
			processorNodeSampleHandlers = new HashMap<>();
		}
		Node<?> node = aspect.getProcessorBaseContext().getNode();
		ProcessorNodeSampleHandler handler = new ProcessorNodeSampleHandler(task, node);
		processorNodeSampleHandlers.put(node.getId(), handler);
		handler.init();

		return null;
	}

	public Void handleProcessorNodeClose(ProcessorNodeCloseAspect aspect) {
		String nodeId = aspect.getProcessorBaseContext().getNode().getId();
		Optional.ofNullable(processorNodeSampleHandlers.get(nodeId)).ifPresent(ProcessorNodeSampleHandler::close);

		return null;
	}

	public Void handleProcessorNodeProcess(ProcessorNodeProcessAspect aspect) {
		String nodeId = aspect.getProcessorBaseContext().getNode().getId();

		switch (aspect.getState()) {
			case ProcessorNodeProcessAspect.STATE_START:
				HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapdataEvent(Collections.singletonList(aspect.getInputEvent()));
				Optional.ofNullable(processorNodeSampleHandlers.get(nodeId)).ifPresent(
						handler -> handler.handleProcessStart(recorder)
				);
				aspect.consumer(event -> {
					if (null == event) {
						return;
					}
					HandlerUtil.EventTypeRecorder inner = HandlerUtil.countTapdataEvent(Collections.singletonList(event));
					Optional.ofNullable(processorNodeSampleHandlers.get(nodeId)).ifPresent(
							handler -> handler.handleProcessAccept(inner)
					);
				});
				break;
			case ProcessorNodeProcessAspect.STATE_END:
				Optional.ofNullable(processorNodeSampleHandlers.get(nodeId)).ifPresent(
						handler -> handler.handleProcessEnd(aspect.getTime(), aspect.getEndTime(), aspect.outputCount())
				);
				break;
		}

		return null;
	}

	@Override
	public List<Class<? extends Aspect>> observeAspects() {
		List<Class<? extends Aspect>> aspects = new ArrayList<>();
		for (Class<?> clazz : observerClassHandlers.keyList()) {
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
		observerClassHandlers.handle(aspect);
	}

	@Override
	public AspectInterceptResult onInterceptAspect(Aspect aspect) {
		return null;
	}
}
