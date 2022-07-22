package io.tapdata.observable;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.aspect.*;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.observable.handler.*;

import java.util.ArrayList;
import java.util.List;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC})
public class ObservableAspectTask extends AspectTask {
	private final ClassHandlers observerClassHandlers = new ClassHandlers();

	private TaskSampleHandler taskSampleHandler;
	private TableSampleHandler tableSampleHandler;
	private DataNodeSampleHandler dataNodeSampleHandler;
	private ProcessorNodeSampleHandler processorNodeSampleHandler;

	public ObservableAspectTask() {
		// data node aspects
		observerClassHandlers.register(DataNodeInitAspect.class, this::handleDataNodeInit);
		observerClassHandlers.register(DataNodeCloseAspect.class, this::handleDataNodeClose);
		// source data node aspects
		observerClassHandlers.register(TableCountAspect.class, this::handleTableCount);
		observerClassHandlers.register(BatchReadFuncAspect.class, this::handleBatchReadFunc);
		observerClassHandlers.register(StreamReadFuncAspect.class, this::handleStreamReadFunc);
		// target data node aspects
		observerClassHandlers.register(WriteRecordFuncAspect.class, this::handleWriteRecordFunc);
		observerClassHandlers.register(CreateTableFuncAspect.class, this::handleCreateTableFunc);

		// processor node aspects
		observerClassHandlers.register(ProcessorNodeInitAspect.class, this::handleProcessorNodeInit);
		observerClassHandlers.register(ProcessorNodeCloseAspect.class, this::handleProcessorNodeClose);
		observerClassHandlers.register(ProcessorNodeProcessAspect.class, this::handleProcessorNodeProcess);
	}


	/**
	 * The task started
	 */
	@Override
	public void onStart() {
		taskSampleHandler = new TaskSampleHandler(task);
		tableSampleHandler = new TableSampleHandler(task);
		dataNodeSampleHandler = new DataNodeSampleHandler(task);
		processorNodeSampleHandler = new ProcessorNodeSampleHandler(task);

		taskSampleHandler.init();
	}

	/**
	 * The task stopped
	 */
	@Override
	public void onStop() {
		taskSampleHandler.close();
	}

	public Void handleDataNodeInit(DataNodeInitAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		String associateId = aspect.getDataProcessorContext().getPdkAssociateId();
		dataNodeSampleHandler.init(node, associateId);

		return null;
	}

	public Void handleDataNodeClose(DataNodeCloseAspect aspect) {
		Node<?> node = aspect.getDataProcessorContext().getNode();
		dataNodeSampleHandler.close(node);
		tableSampleHandler.close(node);

		return null;
	}

	public Void handleTableCount(TableCountAspect aspect) {
		switch (aspect.getState()) {
			case TableCountAspect.STATE_START:
				break;
			case TableCountAspect.STATE_END:
				tableSampleHandler.init(aspect.getDataProcessorContext().getNode(), aspect.getTableCountMap());
				break;
		}
		return null;
	}


	public Void handleCreateTableFunc(CreateTableFuncAspect aspect) {
		switch (aspect.getState()) {
			case TableCountAspect.STATE_START:
				break;
			case TableCountAspect.STATE_END:
				taskSampleHandler.addTable(aspect.getCreateTableEvent().getTable().getName());
				taskSampleHandler.handleCreateTableEnd();
				break;
		}

		return null;
	}

	public Void handleBatchReadFunc(BatchReadFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();
		String table = aspect.getTable().getName();

		switch (aspect.getState()) {
			case BatchReadFuncAspect.STATE_START:
				dataNodeSampleHandler.handleBatchReadStart(nodeId, aspect.getTime());
				aspect.consumer(events -> {
					int size = events.size();
					long newestEventTimestamp = events.get(size - 1).getTapEvent().getTime();
					dataNodeSampleHandler.handleBatchReadAccept(nodeId, System.currentTimeMillis(), size, newestEventTimestamp);
					// batch read should calculate table snapshot insert counter
					tableSampleHandler.incrTableSnapshotInsertTotal(nodeId, table, size);
					taskSampleHandler.handleBatchReadAccept(size);
				});
				break;
			case BatchReadFuncAspect.STATE_END:
				taskSampleHandler.handleBatchReadFuncEnd();
				break;
		}

		return null;
	}

	public Void handleStreamReadFunc(StreamReadFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();

		switch (aspect.getState()) {
			case StreamReadFuncAspect.STATE_START:
				dataNodeSampleHandler.handleStreamReadStreamStart(nodeId, aspect.getTime());
				aspect.consumer(events -> {
					HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countEventType(events);
					long newestEventTimestamp = events.get(events.size() - 1).getTapEvent().getTime();
					dataNodeSampleHandler.handleStreamReadAccept(nodeId, System.currentTimeMillis(), recorder, newestEventTimestamp);
					taskSampleHandler.handleStreamReadAccept(recorder);
				});
				break;
			case StreamReadFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	public Void handleWriteRecordFunc(WriteRecordFuncAspect aspect) {
		String nodeId = aspect.getDataProcessorContext().getNode().getId();

		switch (aspect.getState()) {
			case WriteRecordFuncAspect.STATE_START:
				dataNodeSampleHandler.handleWriteRecordStart(nodeId, aspect.getTime());
				aspect.consumer(result -> {
					dataNodeSampleHandler.handleWriteRecordAccept(nodeId, System.currentTimeMillis(), result);
					taskSampleHandler.handleWriteRecordAccept(result);
				});
				break;
			case WriteRecordFuncAspect.STATE_END:
				break;
		}

		return null;
	}

	public Void handleProcessorNodeInit(ProcessorNodeInitAspect aspect) {
		processorNodeSampleHandler.init(aspect.getProcessorBaseContext().getNode());
		return null;
	}

	public Void handleProcessorNodeClose(ProcessorNodeCloseAspect aspect) {
		Node<?> node = aspect.getProcessorBaseContext().getNode();
		processorNodeSampleHandler.close(node);

		return null;
	}

	public Void handleProcessorNodeProcess(ProcessorNodeProcessAspect aspect) {
		String nodeId = aspect.getProcessorBaseContext().getNode().getId();

		switch (aspect.getState()) {
			case ProcessorNodeProcessAspect.STATE_START:
				processorNodeSampleHandler.handleProcessStart(nodeId, aspect.getTime());
				break;
			case ProcessorNodeProcessAspect.STATE_END:
				processorNodeSampleHandler.handleProcessEnd(nodeId, aspect.getTime(), aspect.getEndTime(),
						aspect.outputCount());
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
