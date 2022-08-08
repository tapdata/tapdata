package io.tapdata.flow.engine.V2.node.hazelcast.data;


import com.hazelcast.jet.core.Inbox;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public class HazelcastVirtualTargetNode extends HazelcastDataBaseNode {

	private final static Logger logger = LogManager.getLogger(HazelcastVirtualTargetNode.class);

	private final AtomicInteger counter = new AtomicInteger(0);


	public HazelcastVirtualTargetNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	public void process(int ordinal, @NotNull Inbox inbox) {
		try {
			if (!inbox.isEmpty()) {
				while (isRunning()) {
					List<TapdataEvent> tapdataEvents = new ArrayList<>();
					final int count = inbox.drainTo(tapdataEvents, 1000);
					if (count > 0) {
						counter.addAndGet(count);
					} else {
						break;
					}
				}
			}
		} catch (Exception e) {
			logger.error("Target process failed {}", e.getMessage(), e);
			throw sneakyThrow(e);
		} finally {
			logger.info("Target process finished, total {}", counter.get());
		}
	}
}
