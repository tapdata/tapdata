package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.jet.core.Inbox;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jackin
 * @date 2022/2/22 2:33 PM
 **/
public class HazelcastPdkSourceAndTargetTableNode extends HazelcastPdkBaseNode {

	private Logger logger = LogManager.getLogger(HazelcastPdkSourceAndTargetTableNode.class);
	private final HazelcastSourcePdkDataNode source;
	private final HazelcastTargetPdkDataNode target;

	private TapdataEvent pendingEvent;
	private ExecutorService sourceConsumer;

	public HazelcastPdkSourceAndTargetTableNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.source = new HazelcastSourcePdkDataNode(dataProcessorContext);
		this.target = new HazelcastTargetPdkDataNode(dataProcessorContext);
		this.sourceConsumer = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
	}

	@Override
	public void doInit(@NotNull Context context) throws Exception {
		super.doInit(context);
		this.target.init(context);
		this.source.init(context);
		this.sourceConsumer.execute(this::startSourceConsumer);
	}

	private void startSourceConsumer() {
		source.startSourceConsumer();
	}

	@Override
	public void doClose() throws Exception {
		this.source.close();
		Optional.ofNullable(this.sourceConsumer).ifPresent(ExecutorService::shutdownNow);
		this.target.close();
		super.doClose();
	}

	@Override
	public void process(int ordinal, @NotNull Inbox inbox) {
		if (null != error) {
			throw new RuntimeException(error);
		}
		this.target.process(ordinal, inbox);
	}
}
