package io.tapdata.entity;

import com.tapdata.cache.ICacheService;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.TapdataShareContext;
import com.tapdata.entity.Connections;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.Job;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.ConverterProvider;
import io.tapdata.common.SettingService;
import io.tapdata.debug.DebugProcessor;
import io.tapdata.milestone.MilestoneService;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TargetContext extends Context {

	private String syncStage;
	private AtomicBoolean offsetLookup = new AtomicBoolean(true);
	private TargetSharedContext targetSharedContext;
	private Object lastInsertOffset;
	private AtomicBoolean running = new AtomicBoolean(true);
	private TapdataShareContext tapdataShareContext;
	private boolean isCloud;
	private ClientMongoOperator tapdataClientOperator;
	private boolean firstWorkerThread = false;

	public TargetContext(Job job, Logger logger, Object offset, Connections sourceConn,
						 Connections targetConn, ClientMongoOperator targetClientOperator,
						 SettingService settingService, DebugProcessor debugProcessor,
						 List<JavaScriptFunctions> javaScriptFunctions, ICacheService cacheService,
						 ConverterProvider converterProvider, TapdataShareContext tapdataShareContext,
						 MilestoneService milestoneService,
						 ConfigurationCenter configurationCenter
	) {
		super(job, logger, offset, settingService, sourceConn, targetConn, debugProcessor, javaScriptFunctions, cacheService, converterProvider, milestoneService, configurationCenter);
		this.tapdataShareContext = tapdataShareContext;
	}

	public TargetContext(List<Stage> stages, Connections connection) {
		super(stages, connection);
	}

	public TargetContext(V1EngineContext context) {
		super(
				context.getJob(),
				context.getLogger(),
				context.getOffset(),
				context.getSettingService(),
				context.getSourceConn(),
				context.getTargetConn(),
				context.getDebugProcessor(),
				context.getJavaScriptFunctions(),
				context.getCacheService(),
				context.getConverterProvider(),
				context.getMilestoneService(),
				context.getDataFlow()
		);
		this.targetSharedContext = new TargetSharedContext();
		this.tapdataClientOperator = context.getClientMongoOperator();
	}

	public TargetContext(V1EngineContext context,
						 TaskDto taskDto,
						 Node<?> node,
						 ConfigurationCenter configurationCenter) {
		super(
				context.getJob(),
				context.getLogger(),
				context.getOffset(),
				context.getSettingService(),
				context.getSourceConn(),
				context.getTargetConn(),
				context.getDebugProcessor(),
				context.getJavaScriptFunctions(),
				context.getCacheService(),
				context.getConverterProvider(),
				context.getMilestoneService(),
				context.getDataFlow(),
				taskDto, node, configurationCenter
		);
		this.targetSharedContext = new TargetSharedContext();
		this.tapdataClientOperator = context.getClientMongoOperator();
	}

	public String getSyncStage() {
		return syncStage;
	}

	public void setSyncStage(String syncStage) {
		this.syncStage = syncStage;
	}

	public TargetSharedContext getTargetSharedContext() {
		return targetSharedContext;
	}

	public void setTargetSharedContext(TargetSharedContext targetSharedContext) {
		this.targetSharedContext = targetSharedContext;
	}

	public AtomicBoolean getOffsetLookup() {
		return offsetLookup;
	}

	public Object getLastInsertOffset() {
		return lastInsertOffset;
	}

	public void setLastInsertOffset(Object lastInsertOffset) {
		this.lastInsertOffset = lastInsertOffset;
	}

	public TapdataShareContext getTapdataShareContext() {
		return tapdataShareContext;
	}

	public void setTapdataShareContext(TapdataShareContext tapdataShareContext) {
		this.tapdataShareContext = tapdataShareContext;
	}

	public boolean isCloud() {
		return isCloud;
	}

	public void setCloud(boolean cloud) {
		isCloud = cloud;
	}

	public ClientMongoOperator getTapdataClientOperator() {
		return tapdataClientOperator;
	}

	public void setTapdataClientOperator(ClientMongoOperator tapdataClientOperator) {
		this.tapdataClientOperator = tapdataClientOperator;
	}

	public boolean isFirstWorkerThread() {
		return firstWorkerThread;
	}

	public void setFirstWorkerThread(boolean firstWorkerThread) {
		this.firstWorkerThread = firstWorkerThread;
	}

	public boolean stop(boolean forceStop) {
		if (forceStop) {
			if (StringUtils.equalsAny(this.getJob().getStatus(),
					ConnectorConstant.RUNNING,
					ConnectorConstant.STOPPING
			)) {
				this.getJob().setStatus(ConnectorConstant.FORCE_STOPPING);
			}
		} else {
			if (StringUtils.equalsAny(this.getJob().getStatus(),
					ConnectorConstant.RUNNING
			)) {
				this.getJob().setStatus(ConnectorConstant.STOPPING);
			}
		}
		running.set(false);
		return running.get();
	}

	public boolean needCleanTarget() {
		return job.getDrop_target() || !job.getKeepSchema();
	}
}
