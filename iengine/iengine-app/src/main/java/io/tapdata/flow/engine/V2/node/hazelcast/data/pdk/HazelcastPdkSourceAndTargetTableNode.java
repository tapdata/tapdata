package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.jet.core.Inbox;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
	public void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		try {
			this.target.init(context);
			this.source.init(context);
		} catch (Exception e) {
			throw new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR, e);
		}
		this.sourceConsumer.execute(this::startSourceConsumer);
	}

	private void startSourceConsumer() {
		source.startSourceConsumer();
	}

	@Override
	public void doClose() throws TapCodeException {
		try {
			this.source.close();
		} catch (Exception e) {
			throw new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR, e);
		}
		Optional.ofNullable(this.sourceConsumer).ifPresent(ExecutorService::shutdownNow);
		try {
			this.target.close();
		} catch (Exception e) {
			throw new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR, e);
		}
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
