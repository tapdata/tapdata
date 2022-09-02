package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.hazelcast.jet.core.Inbox;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.exception.SourceException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author jackin
 * @date 2022/2/18 2:16 PM
 **/
public class HazelcastTaskSourceAndTarget extends HazelcastDataBaseNode {

	private Logger logger = LogManager.getLogger(HazelcastTaskSourceAndTarget.class);

	private HazelcastTaskSource source;

	private HazelcastTaskTarget target;

	private ExecutorService sourceThreadPool;

	public HazelcastTaskSourceAndTarget(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.source = new HazelcastTaskSource(
				DataProcessorContext.newBuilder()
						.withTaskDto(dataProcessorContext.getTaskDto())
						.withNode(dataProcessorContext.getNode())
						.withNodes(dataProcessorContext.getNodes())
						.withEdges(dataProcessorContext.getEdges())
						.withConfigurationCenter(dataProcessorContext.getConfigurationCenter())
						.withSourceConn(dataProcessorContext.getConnections())
						.build()
		);
		this.target = new HazelcastTaskTarget(
				DataProcessorContext.newBuilder()
						.withTaskDto(dataProcessorContext.getTaskDto())
						.withNode(dataProcessorContext.getNode())
						.withNodes(dataProcessorContext.getNodes())
						.withEdges(dataProcessorContext.getEdges())
						.withConfigurationCenter(dataProcessorContext.getConfigurationCenter())
						.withTargetConn(dataProcessorContext.getConnections())
						.withCacheService(dataProcessorContext.getCacheService())
						.build()
		);
	}

	@Override
	protected void doInit(@Nonnull Context context) throws Exception {
		Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
		source.init(context);
		target.init(context);
		sourceThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());

		startSourceWorker();
	}

	public void startSourceWorker() {
		sourceThreadPool.submit(() -> {
			Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
			while (source.running.get()) {
				try {
					final TapdataEvent dataEvent = source.getEventQueue().poll(5, TimeUnit.SECONDS);
					while (source.running.get()) {
						if (offer(dataEvent)) {
							source.dmlCount(dataEvent);
							break;
						}
					}

				} catch (Exception e) {
					logger.error("Source sync failed {}.", e.getMessage(), e);
					throw new SourceException(e, true);
				}
			}
		});
	}

	@Override
	public void doClose() throws Exception {
		Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
		source.close();
		target.close();
	}

	@Override
	public void process(int ordinal, Inbox inbox) {
		Log4jUtil.setThreadContext(dataProcessorContext.getTaskDto());
		target.process(ordinal, inbox);
	}

	public HazelcastTaskSource getSource() {
		return source;
	}

	public HazelcastTaskTarget getTarget() {
		return target;
	}
}
