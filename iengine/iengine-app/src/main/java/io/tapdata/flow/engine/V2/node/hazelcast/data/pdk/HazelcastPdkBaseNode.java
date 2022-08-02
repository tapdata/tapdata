package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.DmlPolicyEnum;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastDataBaseNode;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author samuel
 * @Description
 * @create 2022-05-10 16:57
 **/
public abstract class HazelcastPdkBaseNode extends HazelcastDataBaseNode {
	private final Logger logger = LogManager.getLogger(HazelcastPdkBaseNode.class);
	private static final String TAG = HazelcastPdkBaseNode.class.getSimpleName();
	protected MonitorManager monitorManager;
	protected SyncProgress syncProgress;
	protected String associateId;

	public HazelcastPdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		if (!StringUtils.equalsAnyIgnoreCase(dataProcessorContext.getSubTaskDto().getParentTask().getSyncType(),
				TaskDto.SYNC_TYPE_DEDUCE_SCHEMA, TaskDto.SYNC_TYPE_TEST_RUN)) {
			this.monitorManager = new MonitorManager();
		}
	}

	protected void connectorNodeInit(DataProcessorContext dataProcessorContext) {
		try {
			PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.INIT, () -> getConnectorNode().connectorInit(), TAG);
		} catch (Exception e) {
			throw new RuntimeException("Failed to init pdk connector, database type: " + dataProcessorContext.getDatabaseType() + ", message: " + e.getMessage(), e);
		}
	}

	protected void createPdkConnectorNode(DataProcessorContext dataProcessorContext, HazelcastInstance hazelcastInstance) {
		SubTaskDto subTaskDto = dataProcessorContext.getSubTaskDto();
		Map<String, Object> connectionConfig = dataProcessorContext.getConnectionConfig();
		DatabaseTypeEnum.DatabaseType databaseType = dataProcessorContext.getDatabaseType();
		PdkTableMap pdkTableMap = new PdkTableMap(dataProcessorContext.getTapTableMap());
		PdkStateMap pdkStateMap = new PdkStateMap(dataProcessorContext.getNode().getId(), hazelcastInstance);
		PdkStateMap globalStateMap = PdkStateMap.globalStateMap(hazelcastInstance);
		Node<?> node = dataProcessorContext.getNode();
		ConnectorCapabilities connectorCapabilities = ConnectorCapabilities.create();
		initDmlPolicy(node, connectorCapabilities);
		Map<String, Object> nodeConfig = null;
		if (node instanceof TableNode) {
			nodeConfig = ((TableNode) node).getNodeConfig();
		}
		this.associateId = ConnectorNodeService.getInstance().putConnectorNode(
				PdkUtil.createNode(subTaskDto.getId().toHexString(),
						databaseType,
						clientMongoOperator,
						this.getClass().getSimpleName() + "-" + dataProcessorContext.getNode().getId(),
						connectionConfig,
						nodeConfig,
						pdkTableMap,
						pdkStateMap,
						globalStateMap,
						connectorCapabilities
				)
		);
		processorBaseContext.setPdkAssociateId(this.associateId);
	}

	private void initDmlPolicy(Node<?> node, ConnectorCapabilities connectorCapabilities) {
		if (node instanceof DatabaseNode && null != ((DatabaseNode) node).getDmlPolicy()) {
			DmlPolicy dmlPolicy = ((DatabaseNode) node).getDmlPolicy();
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
			if (this.monitorManager != null) {
				this.monitorManager.close();
			}
			CommonUtils.ignoreAnyError(() -> Optional.ofNullable(dataProcessorContext.getTapTableMap()).ifPresent(TapTableMap::reset), TAG);
			CommonUtils.ignoreAnyError(() -> {
				logger.info("Starting stop and release PDK connector node: " + associateId);
				Optional.ofNullable(getConnectorNode())
						.ifPresent(connectorNode -> PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.STOP, () -> getConnectorNode().connectorStop(), TAG));
				logger.info("PDK connector node stopped: " + associateId);
				Optional.ofNullable(getConnectorNode()).ifPresent(node -> PDKIntegration.releaseAssociateId(associateId));
				logger.info("PDK connector node released: " + associateId);
				ConnectorNodeService.getInstance().removeConnectorNode(associateId);
				logger.info("Release PDK connector node completed: " + associateId);
			}, TAG);
		} finally {
			super.doClose();
		}
	}

	protected ConnectorNode getConnectorNode() {
		return ConnectorNodeService.getInstance().getConnectorNode(associateId);
	}

	protected void tapRecordToTapValue(TapEvent tapEvent, TapCodecsFilterManager codecsFilterManager) {
		if (tapEvent instanceof TapRecordEvent) {
			String tableName;
			tableName = ((TapRecordEvent) tapEvent).getTableId();
			Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
			toTapValue(after, tableName, codecsFilterManager);
			Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
			toTapValue(before, tableName, codecsFilterManager);
		}
	}
}
