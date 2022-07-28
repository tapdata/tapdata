package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.aspect.ProcessorNodeProcessAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiConsumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-12 17:10
 **/
public abstract class HazelcastProcessorBaseNode extends HazelcastBaseNode {
	public HazelcastProcessorBaseNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
	}

	@Override
	protected final boolean tryProcess(int ordinal, @NotNull Object item) throws Exception {
		TapdataEvent tapdataEvent = (TapdataEvent) item;
		AspectUtils.executeProcessorFuncAspect(ProcessorNodeProcessAspect.class, () -> new ProcessorNodeProcessAspect()
				.processorBaseContext(getProcessorBaseContext())
				.inputEvent(tapdataEvent)
				.start(), (processorNodeProcessAspect) -> {
			if (null == tapdataEvent.getTapEvent()) {
				while (running.get()) {
					if (offer(tapdataEvent)) {
						if(processorNodeProcessAspect != null && processorNodeProcessAspect.getConsumer() != null)
							processorNodeProcessAspect.getConsumer().accept(tapdataEvent);
						break;
					}
				}
				return;
			}
			// Update memory from ddl event info map
			updateMemoryFromDDLInfoMap(tapdataEvent);
			if (tapdataEvent.isDML()) {
				transformFromTapValue(tapdataEvent, null);
			}
			tryProcess(tapdataEvent, (event, processResult) -> {
				if (null == event) {
					return;
				}
				if (tapdataEvent.isDML()) {
					if (null != processResult && null != processResult.getTableId()) {
						transformToTapValue(event, processorBaseContext.getTapTableMap(), processResult.getTableId());
					} else {
						transformToTapValue(event, processorBaseContext.getTapTableMap(), getNode().getId());
					}
				}
				while (running.get()) {
					if (offer(event)) {
						if(processorNodeProcessAspect != null && processorNodeProcessAspect.getConsumer() != null)
							processorNodeProcessAspect.getConsumer().accept(event);
						break;
					}
				}
			});
		});
		return true;
	}

	protected abstract void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer);

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
