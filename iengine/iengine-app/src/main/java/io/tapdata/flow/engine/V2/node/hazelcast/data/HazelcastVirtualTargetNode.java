package io.tapdata.flow.engine.V2.node.hazelcast.data;


import com.hazelcast.jet.core.Inbox;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
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
			Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
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
			ThreadContext.clearAll();
			logger.info("Target process finished, total {}", counter.get());
			ThreadContext.clearAll();
		}
	}
}
