package io.tapdata.node.pdk;

import io.tapdata.pdk.core.api.ConnectorNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author samuel
 * @Description
 * @create 2022-07-09 15:15
 **/
public class ConnectorNodeService {
	private final Map<String, ConnectorNode> connectorNodeMap;

	private ConnectorNodeService() {
		this.connectorNodeMap = new ConcurrentHashMap<>();
	}

	public static ConnectorNodeService getInstance() {
		return ConnectorNodeServiceInstance.SINGLETON.getInstance();
	}

	public String putConnectorNode(ConnectorNode connectorNode) {
		if (null == connectorNode || StringUtils.isBlank(connectorNode.getAssociateId())) {
			throw new RuntimeException("Store connector node failed, node is null or associateId is blank");
		}
		this.connectorNodeMap.put(connectorNode.getAssociateId(), connectorNode);
		return connectorNode.getAssociateId();
	}

	public ConnectorNode getConnectorNode(String associateId) {
		if (null == associateId) {
			return null;
		}
		return this.connectorNodeMap.get(associateId);
	}

	public ConnectorNode getConnectorNode(ConnectorNode connectorNode) {
		return this.connectorNodeMap.get(connectorNode.getAssociateId());
	}

	public void removeConnectorNode(String associateId) {
		if (null != associateId) {
			this.connectorNodeMap.remove(associateId);
		}
	}

	private enum ConnectorNodeServiceInstance {
		SINGLETON;
		private final ConnectorNodeService connectorNodeService;

		ConnectorNodeServiceInstance() {
			this.connectorNodeService = new ConnectorNodeService();
		}

		public ConnectorNodeService getInstance() {
			return connectorNodeService;
		}
	}
}
