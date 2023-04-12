package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.ProcessorNodeProcessAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-12 17:10
 **/
public abstract class HazelcastProcessorBaseNode extends HazelcastBaseNode {
	private static final String TAG = HazelcastProcessorBaseNode.class.getSimpleName();

	/**
	 * Ignore process
	 */
	private boolean ignore;

	public HazelcastProcessorBaseNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
	}

	@Override
	protected final boolean tryProcess(int ordinal, @NotNull Object item) throws Exception {
		try {
			if (!isRunning()) {
				return true;
			}
			TapdataEvent tapdataEvent = (TapdataEvent) item;
			List<TapdataEvent> processedEventList = new ArrayList<>();
			try {
				AspectUtils.executeProcessorFuncAspect(ProcessorNodeProcessAspect.class, () -> new ProcessorNodeProcessAspect()
						.processorBaseContext(getProcessorBaseContext())
						.inputEvent(tapdataEvent)
						.start(), (processorNodeProcessAspect) -> {
					if (null == tapdataEvent.getTapEvent() || ignore) {
						// control tapdata event, skip the process consider process is done
						processedEventList.add(tapdataEvent);
						if (null != processorNodeProcessAspect) {
							AspectUtils.accept(processorNodeProcessAspect.state(ProcessorNodeProcessAspect.STATE_PROCESSING).getConsumers(), tapdataEvent);
						}
						return;
					}
					// Update memory from ddl event info map
					updateMemoryFromDDLInfoMap(tapdataEvent, getTgtTableNameFromTapEvent(tapdataEvent.getTapEvent()));
					AtomicReference<TapValueTransform> tapValueTransform = new AtomicReference<>();
					if (tapdataEvent.isDML()) {
						tapValueTransform.set(transformFromTapValue(tapdataEvent));
					}
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
				});
			} catch (Throwable throwable) {
				throw new TapEventException(TaskProcessorExCode_11.UNKNOWN_ERROR, throwable).addEvent(tapdataEvent.getTapEvent());
			}

			if (CollectionUtils.isNotEmpty(processedEventList)) {
				for (TapdataEvent event : processedEventList) {
					while (isRunning()) {
						if (offer(event)) {
							break;
						}
					}
				}
			}
		} catch (Throwable throwable) {
			errorHandle(throwable, throwable.getMessage());
		} finally {
			ThreadContext.clearAll();
		}
		return true;
	}

	@Override
	protected void doClose() throws Exception {
		super.doClose();
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
}
