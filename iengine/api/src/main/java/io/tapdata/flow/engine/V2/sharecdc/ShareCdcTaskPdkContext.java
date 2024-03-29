package io.tapdata.flow.engine.V2.sharecdc;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.pdk.core.api.ConnectorNode;

/**
 * @author samuel
 * @Description
 * @create 2022-06-15 16:15
 **/
public class ShareCdcTaskPdkContext extends ShareCdcTaskContext {
	private final ConnectorNode connectorNode;

	public ShareCdcTaskPdkContext(Long cdcStartTs, ConfigurationCenter configurationCenter, TaskDto taskDto, Node node, Connections connections, ConnectorNode connectorNode) {
		super(cdcStartTs, configurationCenter, taskDto, node, connections);
		this.connectorNode = connectorNode;
	}

	public ConnectorNode getConnectorNode() {
		return connectorNode;
	}
}
