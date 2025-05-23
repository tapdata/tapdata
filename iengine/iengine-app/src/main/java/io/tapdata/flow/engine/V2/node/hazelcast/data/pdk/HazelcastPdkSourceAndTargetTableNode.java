package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.jet.core.Inbox;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataPreviewCompleteEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.exception.SourceAndTargetNodeExCode_38;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jackin
 * @date 2022/2/22 2:33 PM
 **/
public class HazelcastPdkSourceAndTargetTableNode extends HazelcastPdkBaseNode {

	private static final String TAG = HazelcastPdkSourceAndTargetTableNode.class.getSimpleName();
	private HazelcastSourcePdkDataNode source;
	private HazelcastTargetPdkDataNode target;
	private final AtomicBoolean sourceStartFlag = new AtomicBoolean(false);
	private ScheduledExecutorService callCompleteMethodThreadPool;

	public HazelcastPdkSourceAndTargetTableNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		if (dataProcessorContext.getTaskDto().isNormalTask()) {
			dataProcessorContext.setSourceThreadGroupName(String.join("_", ConnectorOnTaskThreadGroup.class.getSimpleName(), "S", getNode().getId(), getNode().getName()));
			dataProcessorContext.setTargetThreadGroupName(String.join("_", ConnectorOnTaskThreadGroup.class.getSimpleName(), "T", getNode().getId(), getNode().getName()));
			this.source = new HazelcastSourcePdkDataNode(dataProcessorContext);
			this.target = new HazelcastTargetPdkDataNode(dataProcessorContext);
			this.callCompleteMethodThreadPool = new ScheduledThreadPoolExecutor(1);
		}
	}

	@Override
	public void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		if (taskDto.isNormalTask()) {
			startTarget(context);
			boolean sourceHaveSyncProgress = isSourceHaveSyncProgress(taskDto);
			if (TaskDto.TYPE_CDC.equals(taskDto.getType()) || sourceHaveSyncProgress) {
				startSource(context);
			}
		}
	}

	protected boolean isSourceHaveSyncProgress(TaskDto taskDto) {
		boolean sourceHaveSyncProgress = false;
		String nodeId = getNode().getId();
		Map<String, SyncProgress> allSyncProgress = foundAllSyncProgress(taskDto.getAttrs());
		for (Map.Entry<String, SyncProgress> entry : allSyncProgress.entrySet()) {
			String key = entry.getKey();
			String[] nodeIds = key.split(",");
			if (nodeId.equals(nodeIds[0])) {
				sourceHaveSyncProgress = true;
				break;
			}
		}
		return sourceHaveSyncProgress;
	}

	protected void startTarget(@NotNull Context context) {
		try {
			this.target.init(context);
			this.target.targetAllInitialCompleteNotify(this::targetAllInitialCompleteNotifyExecute);
		} catch (Exception e) {
			TapCodeException tapCodeException = new TapCodeException(SourceAndTargetNodeExCode_38.INIT_TARGET_ERROR, e).dynamicDescriptionParameters(getNode().getName(), dataProcessorContext.getTargetConn().getName());
			errorHandle(tapCodeException);
		}
	}

	protected void startSource(@NotNull Context context) {
		if (this.sourceStartFlag.compareAndSet(false, true)) {
			try {
				try {
					TimeUnit.SECONDS.sleep(1L);
				} catch (InterruptedException e) {
					return;
				}
				this.source.init(context);
				this.source.setHazelcastTaskNodeOffer(this::offer);
				this.callCompleteMethodThreadPool.scheduleWithFixedDelay(() -> {
					if (complete()) {
						this.callCompleteMethodThreadPool.shutdownNow();
					}
				}, 0L, 10L, TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				this.sourceStartFlag.set(false);
				TapCodeException tapCodeException = new TapCodeException(SourceAndTargetNodeExCode_38.INIT_SOURCE_ERROR, e).dynamicDescriptionParameters(getNode().getName(), dataProcessorContext.getSourceConn().getName());
				errorHandle(tapCodeException);
			}
		}
	}

	protected boolean offer(TapdataEvent tapdataEvent) {
		return super.offer(tapdataEvent);
	}

	@Override
	public boolean complete() {
		if (!running.get()) {
			return true;
		}
		if (this.sourceStartFlag.get()) {
			return this.source.complete();
		}
		return false;
	}

	@Override
	public void doClose() throws TapCodeException {
		CommonUtils.ignoreAnyError(() -> {
			if (null != this.source) {
				this.source.close();
			}
		}, TAG);
		CommonUtils.ignoreAnyError(() -> {
			if (null != this.target) {
				this.target.close();
			}
		}, TAG);
		super.doClose();
	}

	@Override
	public void process(int ordinal, @NotNull Inbox inbox) {
		if (null != this.target) {
			this.target.process(ordinal, inbox);
		} else {
			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			int drainTo = inbox.drainTo(tapdataEvents);
			if (drainTo > 0) {
				tapdataEvents.forEach(tapdataEvent -> {
					offer(tapdataEvent);
					if (tapdataEvent instanceof TapdataPreviewCompleteEvent) {
						this.running.set(false);
					}
				});
			}
		}
	}

	public void targetAllInitialCompleteNotifyExecute() {
		startSource(jetContext);
		obsLogger.info("Full synchronization of the front pipeline has been completed and subsequent reading is started");
	}
}
