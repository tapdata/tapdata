package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.DmlPolicyEnum;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.PDKNodeInitAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.log.LogFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastDataBaseNode;
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
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.PdkTableMap;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2022-05-10 16:57
 **/
public abstract class HazelcastPdkBaseNode extends HazelcastDataBaseNode {
	public static final int DEFAULT_READ_BATCH_SIZE = 2000;
	private final Logger logger = LogManager.getLogger(HazelcastPdkBaseNode.class);
	private static final String TAG = HazelcastPdkBaseNode.class.getSimpleName();
	protected static final String COMPLETED_INITIAL_SYNC_KEY_PREFIX = "COMPLETED-INITIAL-SYNC-";
	protected SyncProgress syncProgress;
	protected String associateId;
	protected TapLogger.LogListener logListener;
	private final List<PDKMethodInvoker> pdkMethodInvokerList = new ArrayList<>();

	protected Integer readBatchSize;

	public HazelcastPdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
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
	}

	public PDKMethodInvoker createPdkMethodInvoker() {
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		TaskConfig taskConfig = dataProcessorContext.getTaskConfig();
		Long retryIntervalSecond = taskConfig.getTaskRetryConfig().getRetryIntervalSecond();
		long retryIntervalMs = TimeUnit.SECONDS.toMillis(retryIntervalSecond);
		Long maxRetryTimeSecond = taskConfig.getTaskRetryConfig().getMaxRetryTime(TimeUnit.SECONDS);
		long retryDurationMs = TimeUnit.SECONDS.toMillis(maxRetryTimeSecond);
		TaskRetryService taskRetryService = TaskRetryFactory.getInstance().getTaskRetryService(taskDto, retryDurationMs);
		if (maxRetryTimeSecond > 0) {
			long methodRetryDurationMs = taskRetryService.getMethodRetryDurationMs(retryIntervalMs);
			maxRetryTimeSecond = Math.max(TimeUnit.MILLISECONDS.toMinutes(methodRetryDurationMs), 1L);
		} else {
			maxRetryTimeSecond = 0L;
		}
		PDKMethodInvoker pdkMethodInvoker = PDKMethodInvoker.create()
				.logTag(TAG)
				.retryPeriodSeconds(retryIntervalSecond)
				.maxRetryTimeMinute(maxRetryTimeSecond)
				.logListener(logListener)
				.startRetry(taskRetryService::start)
				.resetRetry(taskRetryService::reset);
		this.pdkMethodInvokerList.add(pdkMethodInvoker);
		return pdkMethodInvoker;
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
		PdkStateMap pdkStateMap = new PdkStateMap(dataProcessorContext.getNode().getId(), hazelcastInstance, taskDto, getNode(), clientMongoOperator, "processor");
		PdkStateMap globalStateMap = PdkStateMap.globalStateMap(hazelcastInstance);
		Node<?> node = dataProcessorContext.getNode();
		ConnectorCapabilities connectorCapabilities = ConnectorCapabilities.create();
		initDmlPolicy(node, connectorCapabilities);
		Map<String, Object> nodeConfig = null;
		if (node instanceof TableNode) {
			nodeConfig = ((TableNode) node).getNodeConfig();
		} else if (node instanceof DatabaseNode) {
			nodeConfig = ((DatabaseNode) node).getNodeConfig();
		}
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

	private void initDmlPolicy(Node<?> node, ConnectorCapabilities connectorCapabilities) {
		if (node instanceof DataParentNode && null != ((DataParentNode<?>) node).getDmlPolicy()) {
			DmlPolicy dmlPolicy = ((DataParentNode<?>) node).getDmlPolicy();
			DmlPolicyEnum insertPolicy = null == dmlPolicy.getInsertPolicy() ? DmlPolicyEnum.update_on_exists : dmlPolicy.getInsertPolicy();
			if (insertPolicy == DmlPolicyEnum.ignore_on_exists) {
				connectorCapabilities.alternative(ConnectionOptions.DML_INSERT_POLICY, ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS);
			} else {
				connectorCapabilities.alternative(ConnectionOptions.DML_INSERT_POLICY, ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS);
			}
			DmlPolicyEnum updatePolicy = null == dmlPolicy.getUpdatePolicy() ? DmlPolicyEnum.ignore_on_nonexists : dmlPolicy.getUpdatePolicy();
			if (updatePolicy == DmlPolicyEnum.insert_on_nonexists) {
				connectorCapabilities.alternative(ConnectionOptions.DML_UPDATE_POLICY, ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS);
			} else {
				connectorCapabilities.alternative(ConnectionOptions.DML_UPDATE_POLICY, ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS);
			}
		} else {
			// Default
			connectorCapabilities.alternative(ConnectionOptions.DML_INSERT_POLICY, ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS);
			connectorCapabilities.alternative(ConnectionOptions.DML_UPDATE_POLICY, ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS);
		}
	}

	protected void toTapValue(Map<String, Object> data, String tableName, TapCodecsFilterManager tapCodecsFilterManager) {
		if (MapUtils.isEmpty(data)) {
			return;
		}
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(tableName);
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		tapCodecsFilterManager.transformToTapValueMap(data, nameFieldMap);
	}

	protected void fromTapValue(Map<String, Object> data, TapCodecsFilterManager tapCodecsFilterManager) {
		if (MapUtils.isEmpty(data)) {
			return;
		}
		tapCodecsFilterManager.transformFromTapValueMap(data);
	}

	@Override
	public void doClose() throws Exception {
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

	protected void tapRecordToTapValue(TapEvent tapEvent, TapCodecsFilterManager codecsFilterManager) {
		if (tapEvent instanceof TapRecordEvent) {
			TapRecordEvent tapRecordEvent = (TapRecordEvent) tapEvent;
			String tableName = ShareCdcUtil.getTapRecordEventTableName(tapRecordEvent);
			Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
			toTapValue(after, tableName, codecsFilterManager);
			Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
			toTapValue(before, tableName, codecsFilterManager);
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
}
