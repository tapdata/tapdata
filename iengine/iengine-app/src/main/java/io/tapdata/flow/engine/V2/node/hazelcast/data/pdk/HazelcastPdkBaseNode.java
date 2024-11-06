package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import cn.hutool.core.collection.ConcurrentHashSet;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.DmlPolicyEnum;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.PDKNodeInitAspect;
import io.tapdata.aspect.taskmilestones.RetryLifeCycleAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.codec.filter.TapCodecsFilterManagerSchemaEnforced;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.exception.TapCodeException;
import com.tapdata.entity.TransformToTapValueResult;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMapEx;
import io.tapdata.flow.engine.V2.filter.TapRecordSkipDetector;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastDataBaseNode;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryContext;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryFactory;
import io.tapdata.flow.engine.V2.task.retry.task.TaskRetryService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.error.TapPdkRunnerUnknownException;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.RetryLifeCycle;
import io.tapdata.pdk.core.utils.SampleRetryLifeCycle;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import io.tapdata.supervisor.TaskNodeInfo;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author samuel
 * @Description
 * @create 2022-05-10 16:57
 **/
public abstract class HazelcastPdkBaseNode extends HazelcastDataBaseNode {
	public static final int DEFAULT_READ_BATCH_SIZE = 100;
	public static final int DEFAULT_INCREASE_BATCH_SIZE = 1;
	public static final String OLD_VERSION_TIMEZONE = "oldVersionTimezone";
	public static final String OLD_VERSION_TIME_ZONE_PROP_KEY = "OLD_VERSION_TIME_ZONE";
	private final Logger logger = LogManager.getLogger(HazelcastPdkBaseNode.class);
	private static final String TAG = HazelcastPdkBaseNode.class.getSimpleName();
	protected static final String COMPLETED_INITIAL_SYNC_KEY_PREFIX = "COMPLETED-INITIAL-SYNC-";
	protected String associateId;
	protected TapLogger.LogListener logListener;
	private final List<PDKMethodInvoker> pdkMethodInvokerList = new CopyOnWriteArrayList<>();

	protected Integer readBatchSize;
	protected Integer increaseReadSize;

	public static final String FUNCTION_RETRY_STATUS = "functionRetryStatus";
	private static final String DOUBLE_ACTIVE = "doubleActive";
	private static final String WRITE_THREAD_SIZE = "writeThreadSize";
	protected TapRecordSkipDetector skipDetector;
	private PdkStateMap pdkStateMap;

	protected TapRecordSkipDetector getSkipDetector() {
		return skipDetector;
	}

	public HazelcastPdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		logListener = new TapLogger.LogListener() {
			@Override
			public void debug(String log) {
				obsLogger.debug(log);
			}

			@Override
			public void info(String log) {
				obsLogger.info(log);
			}

			@Override
			public void warn(String log) {
				obsLogger.warn(log);
			}

			@Override
			public void error(String log) {
				obsLogger.error(log);
			}

			@Override
			public void fatal(String log) {
				obsLogger.fatal(log);
			}

			@Override
			public void memory(String memoryLog) {
				info(memoryLog);
			}
		};
		skipDetector = getIsomorphism() ? new TapRecordSkipDetector() : null;
	}

	public PDKMethodInvoker createPdkMethodInvoker() {
		TaskRetryService taskRetryService = TaskRetryFactory.getInstance().getTaskRetryService(processorBaseContext);
		TaskRetryContext retryContext = (TaskRetryContext) taskRetryService.getRetryContext();
		TaskDto taskDto = retryContext.getTaskDto();
		long retryIntervalSecond = TimeUnit.MILLISECONDS.toSeconds(retryContext.getRetryIntervalMs());
		long methodRetryTimeMintues = taskRetryService.getMethodRetryDurationMinutes();
		PDKMethodInvoker pdkMethodInvoker = PDKMethodInvoker.create()
				.logTag(TAG)
				.retryPeriodSeconds(retryIntervalSecond)
				.maxRetryTimeMinute(methodRetryTimeMintues)
				.logListener(logListener)
				.startRetry(taskRetryService::start)
				.resetRetry(taskRetryService::reset)
				.signFunctionRetry(() -> signFunctionRetry(taskDto.getId().toHexString()))
				.clearFunctionRetry(() -> cleanFuctionRetry(taskDto.getId().toHexString()))
				.retryLifeCycle(createRetryLifeCycle());
		this.pdkMethodInvokerList.add(pdkMethodInvoker);
		return pdkMethodInvoker;
	}

	public void signFunctionRetry(String taskId) {
		CommonUtils.ignoreAnyError(() -> {
			Update update = new Update();
			update.set(FUNCTION_RETRY_STATUS, TaskDto.RETRY_STATUS_RUNNING);
			update.set("taskRetryStartTime", System.currentTimeMillis());
			Criteria functionRetryStatusExists = Criteria.where(FUNCTION_RETRY_STATUS).exists(false);
			Criteria functionRetryStatusNone = Criteria.where(FUNCTION_RETRY_STATUS).is(TaskDto.RETRY_STATUS_NONE);
			Query query = Query.query(Criteria.where("_id").is(new ObjectId(taskId))
					.orOperator(functionRetryStatusExists, functionRetryStatusNone));
			clientMongoOperator.update(query, update, ConnectorConstant.TASK_COLLECTION);
		}, "Faild to sign function retry status");
	}

	public void cleanFuctionRetry(String taskId) {
		CommonUtils.ignoreAnyError(() -> {
			Update update = new Update();
			update.set(FUNCTION_RETRY_STATUS, TaskDto.RETRY_STATUS_NONE);
			update.set("taskRetryStartTime", 0);
			clientMongoOperator.update(Query.query(Criteria.where("_id").is(new ObjectId(taskId))), update, ConnectorConstant.TASK_COLLECTION);
		}, "Faild to clean function retry status");
	}



	public void removePdkMethodInvoker(PDKMethodInvoker pdkMethodInvoker) {
		if (null == pdkMethodInvoker) return;
		pdkMethodInvokerList.remove(pdkMethodInvoker);
	}

	protected void connectorNodeInit(DataProcessorContext dataProcessorContext) {
		try {
			PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.INIT, () -> getConnectorNode().connectorInit(), TAG);
		} catch (Exception e) {
			throw new RuntimeException("Failed to init pdk connector, database type: " + dataProcessorContext.getDatabaseType() + ", message: " + e.getMessage(), e);
		}
	}
	protected void createPdkConnectorNode(DataProcessorContext dataProcessorContext, HazelcastInstance hazelcastInstance) {
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		Map<String, Object> connectionConfig = dataProcessorContext.getConnectionConfig();
		DatabaseTypeEnum.DatabaseType databaseType = dataProcessorContext.getDatabaseType();
		PdkTableMap pdkTableMap = new PdkTableMap(dataProcessorContext.getTapTableMap());
		pdkStateMap = new PdkStateMapEx(hazelcastInstance, getNode());
		PdkStateMap globalStateMap = PdkStateMap.globalStateMap(hazelcastInstance);
		Node<?> node = dataProcessorContext.getNode();
		ConnectorCapabilities connectorCapabilities = ConnectorCapabilities.create();
		initDmlPolicy(node, connectorCapabilities);
		Map<String, Object> nodeConfig = generateNodeConfig(node, taskDto);
		this.associateId = ConnectorNodeService.getInstance().putConnectorNode(
				PdkUtil.createNode(taskDto.getId().toHexString(),
						databaseType,
						clientMongoOperator,
						this.getClass().getSimpleName() + "-" + dataProcessorContext.getNode().getId(),
						connectionConfig,
						nodeConfig,
						pdkTableMap,
						pdkStateMap,
						globalStateMap,
						connectorCapabilities,
						() -> Log4jUtil.setThreadContext(taskDto),
						new StopTaskOnErrorLog(InstanceFactory.instance(LogFactory.class).getLog(processorBaseContext), this)
				)
		);
		logger.info(String.format("Create PDK connector on node %s[%s] complete | Associate id: %s", getNode().getName(), getNode().getId(), associateId));
		processorBaseContext.setPdkAssociateId(this.associateId);
		AspectUtils.executeAspect(PDKNodeInitAspect.class, () -> new PDKNodeInitAspect().dataProcessorContext((DataProcessorContext) processorBaseContext));
	}

	protected Map<String, Object> generateNodeConfig(Node<?> node, TaskDto taskDto) {
		Map<String, Object> nodeConfig = null;
		if (node instanceof TableNode) {
			nodeConfig = ((TableNode) node).getNodeConfig();
		} else if (node instanceof DatabaseNode) {
			nodeConfig = ((DatabaseNode) node).getNodeConfig();
		} else if (node instanceof LogCollectorNode) {
			nodeConfig = ((LogCollectorNode) node).getNodeConfig();
		}
		if (null == nodeConfig) {
			nodeConfig = new HashMap<>();
		}
        if (node instanceof DataParentNode) {
            Integer initialConcurrentWriteNum = Optional.ofNullable(((DataParentNode<?>) node).getInitialConcurrentWriteNum()).orElse(1);
			Integer cdcConcurrentWriteNum = Optional.ofNullable(((DataParentNode<?>) node).getCdcConcurrentWriteNum()).orElse(1);
            nodeConfig.put(WRITE_THREAD_SIZE, Math.max(initialConcurrentWriteNum, cdcConcurrentWriteNum));
        }
		nodeConfig.put(DOUBLE_ACTIVE, taskDto.getDoubleActive());
		Boolean oldVersionTimezone = taskDto.getOldVersionTimezone();
		oldVersionTimezone = CommonUtils.getPropertyBool(OLD_VERSION_TIME_ZONE_PROP_KEY, oldVersionTimezone);
		nodeConfig.put(OLD_VERSION_TIMEZONE, oldVersionTimezone);
		return nodeConfig;
	}

	protected void initDmlPolicy(Node<?> node, ConnectorCapabilities connectorCapabilities) {
		if (node instanceof DataParentNode && null != ((DataParentNode<?>) node).getDmlPolicy()) {
			DmlPolicy dmlPolicy = ((DataParentNode<?>) node).getDmlPolicy();
			DmlPolicyEnum insertPolicy = null == dmlPolicy.getInsertPolicy() ? DmlPolicyEnum.update_on_exists : dmlPolicy.getInsertPolicy();
			if (insertPolicy == DmlPolicyEnum.ignore_on_exists) {
				connectorCapabilities.alternative(ConnectionOptions.DML_INSERT_POLICY, ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS);
			} else if (insertPolicy == DmlPolicyEnum.update_on_exists) {
				connectorCapabilities.alternative(ConnectionOptions.DML_INSERT_POLICY, ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS);
			} else {
				connectorCapabilities.alternative(ConnectionOptions.DML_INSERT_POLICY, ConnectionOptions.DML_INSERT_POLICY_JUST_INSERT);
			}
			DmlPolicyEnum updatePolicy = null == dmlPolicy.getUpdatePolicy() ? DmlPolicyEnum.ignore_on_nonexists : dmlPolicy.getUpdatePolicy();
			if (updatePolicy == DmlPolicyEnum.insert_on_nonexists) {
				connectorCapabilities.alternative(ConnectionOptions.DML_UPDATE_POLICY, ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS);
			} else if (updatePolicy == DmlPolicyEnum.ignore_on_nonexists) {
				connectorCapabilities.alternative(ConnectionOptions.DML_UPDATE_POLICY, ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS);
			} else {
				connectorCapabilities.alternative(ConnectionOptions.DML_UPDATE_POLICY, ConnectionOptions.DML_UPDATE_POLICY_IGNORE_LOG_ON_NON_EXISTS);
			}
		} else {
			// Default
			connectorCapabilities.alternative(ConnectionOptions.DML_INSERT_POLICY, ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS);
			connectorCapabilities.alternative(ConnectionOptions.DML_UPDATE_POLICY, ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS);
		}
	}

	protected Set<String> toTapValue(Map<String, Object> data, String tableName, TapCodecsFilterManager tapCodecsFilterManager) {
		if (MapUtils.isEmpty(data) || null == tapCodecsFilterManager || null == tableName) {
			return null;
		}
		if (tapCodecsFilterManager instanceof TapCodecsFilterManagerSchemaEnforced) {
			return ((TapCodecsFilterManagerSchemaEnforced) tapCodecsFilterManager).transformToTapValueMap(data, dataProcessorContext.getTapTableMap().get(tableName), getSkipDetector());
		} else {
			tapCodecsFilterManager.transformToTapValueMap(data, getTableFiledMap(tableName), getSkipDetector());
		}
		return null;
	}

	protected LinkedHashMap<String, TapField> getTableFiledMap(String tableName) {
		if (null == tableName) return new LinkedHashMap<>();
		DataProcessorContext dataProcessorContext = getDataProcessorContext();
		if (null == dataProcessorContext) return new LinkedHashMap<>();
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		if (null == tapTableMap) return new LinkedHashMap<>();
		TapTable tapTable = tapTableMap.get(tableName);
		if (null == tapTable) return new LinkedHashMap<>();
		return tapTable.getNameFieldMap();
	}

	protected void fromTapValue(Map<String, Object> data, TapCodecsFilterManager tapCodecsFilterManager, String targetTableName) {
		if (MapUtils.isEmpty(data) || null == tapCodecsFilterManager || null == targetTableName) {
			return;
		}
		tapCodecsFilterManager.transformFromTapValueMap(data, getTableFiledMap(targetTableName), getSkipDetector());
	}

	protected void fromTapValue(Map<String, Object> data, TapCodecsFilterManager tapCodecsFilterManager, TapTable tapTable) {
		if (MapUtils.isEmpty(data) || null == tapCodecsFilterManager || null == tapTable) {
			return;
		}
		tapCodecsFilterManager.transformFromTapValueMap(data, tapTable.getNameFieldMap(), getSkipDetector());
	}

	protected void fromTapValue(
			Map<String, Object> data,
			TapCodecsFilterManager tapCodecsFilterManager,
			String targetTableName,
			Set<String> transformedToTapValueFieldNames
	) {
		if (MapUtils.isEmpty(data) || null == tapCodecsFilterManager || null == targetTableName) {
			return;
		}
		LinkedHashMap<String, TapField> tableFiledMap = getTableFiledMap(targetTableName);
		if (tapCodecsFilterManager instanceof TapCodecsFilterManagerSchemaEnforced) {
			((TapCodecsFilterManagerSchemaEnforced) tapCodecsFilterManager).transformFromTapValueMap(
					data,
					tableFiledMap,
					transformedToTapValueFieldNames
			);
		} else {
			tapCodecsFilterManager.transformFromTapValueMap(data, tableFiledMap, getSkipDetector());
		}
	}

	@Override
	public void doClose() throws TapCodeException {
		try {
			CommonUtils.ignoreAnyError(() -> {
				if (null != pdkMethodInvokerList) {
					for (PDKMethodInvoker pdkMethodInvoker : pdkMethodInvokerList) {
						if (null == pdkMethodInvoker) continue;
						pdkMethodInvoker.cancelRetry();
					}
				}
			}, TAG);
			CommonUtils.handleAnyError(() -> {
				Optional.ofNullable(getConnectorNode())
						.ifPresent(connectorNode -> {
							PDKInvocationMonitor.stop(getConnectorNode());
							PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.STOP, () -> getConnectorNode().connectorStop(), TAG);
						});
				if (null != pdkStateMap) {
					pdkStateMap.reset();
				}
				obsLogger.info("PDK connector node stopped: " + associateId);
			}, err -> {
				obsLogger.warn(String.format("Stop PDK connector node failed: %s | Associate id: %s", err.getMessage(), associateId));
			});
			CommonUtils.handleAnyError(() -> {
				Optional.ofNullable(getConnectorNode()).ifPresent(node -> PDKIntegration.releaseAssociateId(associateId));
				ConnectorNodeService.getInstance().removeConnectorNode(associateId);
				obsLogger.info("PDK connector node released: " + associateId);
			}, err -> {
				obsLogger.warn(String.format("Release PDK connector node failed: %s | Associate id: %s", err.getMessage(), associateId));
			});
		} finally {
			super.doClose();
		}
	}

	public ConnectorNode getConnectorNode() {
		return ConnectorNodeService.getInstance().getConnectorNode(associateId);
	}

	protected TransformToTapValueResult tapRecordToTapValue(TapEvent tapEvent, TapCodecsFilterManager codecsFilterManager) {
		TransformToTapValueResult transformToTapValueResult = null;
		if (tapEvent instanceof TapRecordEvent) {
			transformToTapValueResult = TransformToTapValueResult.create();
			TapRecordEvent tapRecordEvent = (TapRecordEvent) tapEvent;
			String tableName = ShareCdcUtil.getTapRecordEventTableName(tapRecordEvent);
			Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
			transformToTapValueResult.afterTransformedToTapValueFieldNames(toTapValue(after, tableName, codecsFilterManager));
			Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
			transformToTapValueResult.beforeTransformedToTapValueFieldNames(toTapValue(before, tableName, codecsFilterManager));
			if (null == transformToTapValueResult.getBeforeTransformedToTapValueFieldNames()
					&& null == transformToTapValueResult.getAfterTransformedToTapValueFieldNames()) {
				transformToTapValueResult = null;
			}
		}
		return transformToTapValueResult;
	}

	protected void throwTapCodeException(Throwable e, TapCodeException tapCodeException) {
		Throwable throwable = CommonUtils.matchThrowable(e, TapCodeException.class);
		if (throwable instanceof TapPdkRunnerUnknownException) {
			throw tapCodeException;
		} else {
			if (null == throwable) {
				throw tapCodeException;
			} else {
				throw (TapCodeException)throwable;
			}
		}
	}

	protected String getCompletedInitialKey() {
		return COMPLETED_INITIAL_SYNC_KEY_PREFIX + dataProcessorContext.getTaskDto().getId().toHexString();
	}

	private PdkStateMap globalMap() {
		return PdkStateMap.globalStateMap(jetContext.hazelcastInstance());
	}

	protected Object getGlobalMap(String key) {
		return globalMap().get(key);
	}

	protected void putInGlobalMap(String key, Object value) {
		globalMap().put(key, value);
	}

	protected void removeGlobalMap(String key) {
		globalMap().remove(key);
	}

	protected void removeNotSupportFields(TapEvent tapEvent, String tableName) {
		removeNotSupportFields(tableName, TapEventUtil.getAfter(tapEvent));
		removeNotSupportFields(tableName, TapEventUtil.getBefore(tapEvent));
	}

	protected void removeNotSupportFields(String tableName, Map<String, Object> data) {
		Map<String, List<String>> notSupportFieldMap = getNode().getNotSupportFieldMap();
		if (StringUtils.isEmpty(tableName) || MapUtils.isEmpty(data) || MapUtils.isEmpty(notSupportFieldMap) || notSupportFieldMap.get(tableName) == null) {
			return;
		}
		List<String> notSupportFieldList = notSupportFieldMap.get(tableName);
		for (String notSupportField : notSupportFieldList) {
			if (obsLogger.isDebugEnabled()) {
				obsLogger.debug("remove not support field [{}] from data [{}]", notSupportField, data);
			}
			MapUtil.removeKey(data, notSupportField);
		}
	}
	protected void readBatchOffset(SyncProgress syncProgress) {
		if (null == syncProgress) {
			return;
		}
		String batchOffset = syncProgress.getBatchOffset();
		if (StringUtils.isNotBlank(batchOffset)) {
			syncProgress.setBatchOffsetObj(PdkUtil.decodeOffset(batchOffset, getConnectorNode()));
		} else {
			syncProgress.setBatchOffsetObj(new HashMap<>());
		}
		cleanTableBatchOffsetIfNeed(syncProgress);
	}

	private void cleanTableBatchOffsetIfNeed(SyncProgress syncProgress) {
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		Object batchOffsetObj = syncProgress.getBatchOffsetObj();
		if (batchOffsetObj instanceof Map) {
			Set<String> tableIds = tapTableMap.keySet();
			Iterator<?> iterator = ((Map<?, ?>) batchOffsetObj).keySet().iterator();
			while (iterator.hasNext()) {
				Object next = iterator.next();
				if (!tableIds.contains(next)) {
					iterator.remove();
				}
			}
			syncProgress.setBatchOffset(PdkUtil.encodeOffset(syncProgress.getBatchOffsetObj()));
		}
	}


	protected ThreadGroup getReuseOrNewThreadGroup(ConcurrentHashSet<TaskNodeInfo> taskNodeInfos) {
		Optional<TaskNodeInfo> leakTaskNodeInfo = taskNodeInfos.stream().filter(
						(taskNodeInfo) -> getNode().getId().equals(taskNodeInfo.getNode().getId())
								&& taskNodeInfo.isHasLaked())
				.findAny();
		AtomicReference<ThreadGroup> connectorOnTaskThreadGroup = new AtomicReference<>();
		leakTaskNodeInfo.ifPresent(taskNodeInfo -> {
			synchronized (taskNodeInfo.getTaskNodeInfoLock()) {
				if (!taskNodeInfo.isHasLaked()) return;
				taskNodeInfo.setHasLeaked(false);
				ThreadGroup nodeThreadGroup = taskNodeInfo.getNodeThreadGroup();
				if (nodeThreadGroup instanceof ConnectorOnTaskThreadGroup) {
					taskNodeInfo.setNode(getNode());
					((ConnectorOnTaskThreadGroup) nodeThreadGroup).setDataProcessorContext(dataProcessorContext);
					connectorOnTaskThreadGroup.set(nodeThreadGroup);
				}
			}
		});
		if (connectorOnTaskThreadGroup.get() == null) {
			connectorOnTaskThreadGroup.set(new ConnectorOnTaskThreadGroup(dataProcessorContext));
		}
		return connectorOnTaskThreadGroup.get();
	}

	public RetryLifeCycle createRetryLifeCycle() {
		return new SampleRetryLifeCycle() {
			@Override
			public void onChange() {
				boolean retrying = startRetryTs.get() > 0 && success == null;
				long retryTimes = this.retryTimes.get();
				long startRetryTs = this.startRetryTs.get();
				Long endRetryTs = this.endRetryTs.get() > 0 ? this.endRetryTs.get() : null;
				Long nextRetryTs = getNextRetryTimestamp();
				Long totalRetries = this.totalRetries.get();
				String retryOp = this.retryOp;
				Boolean success = this.success;

				AspectUtils.executeDataFuncAspect(RetryLifeCycleAspect.class, () -> {
					RetryLifeCycleAspect aspect = new RetryLifeCycleAspect();

					aspect.setRetrying(retrying);
					aspect.setRetryTimes(retryTimes);
					aspect.setStartRetryTs(startRetryTs);
					aspect.setEndRetryTs(endRetryTs);
					aspect.setNextRetryTs(nextRetryTs);
					aspect.setTotalRetries(totalRetries);
					aspect.setRetryOp(retryOp);
					aspect.setSuccess(success);

					return aspect;
				}, aspect -> {
					aspect.dataProcessorContext(getDataProcessorContext());
				});
			}
		};
	}
}
