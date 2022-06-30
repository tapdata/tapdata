package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
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
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.schema.PdkTableMap;
import org.apache.commons.collections4.MapUtils;
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
	protected ConnectorNode connectorNode;
	protected SyncProgress syncProgress;

	public HazelcastPdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.monitorManager = new MonitorManager();
	}

	protected void connectorNodeInit(DataProcessorContext dataProcessorContext) {
		try {
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, () -> connectorNode.connectorInit(), TAG);
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
		this.connectorNode = PdkUtil.createNode(subTaskDto.getId().toHexString(),
				databaseType,
				clientMongoOperator,
				this.getClass().getSimpleName() + "-" + dataProcessorContext.getNode().getId(),
				connectionConfig,
				pdkTableMap,
				pdkStateMap,
				globalStateMap
		);
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
	public void close() throws Exception {
		this.monitorManager.close();
		Optional.ofNullable(connectorNode).ifPresent(node -> PDKIntegration.releaseAssociateId(node.getAssociateId()));
		super.close();
	}

	public ConnectorNode getConnectorNode() {
		return connectorNode;
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
