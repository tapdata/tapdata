package io.tapdata.entity;

import com.tapdata.entity.Connections;

/**
 * @author samuel
 * @Description
 * @create 2024-11-14 17:39
 **/
public class GlobalConnectorResult {
	private Connections connections;
	private String associateId;
	private long connectorNodeInitTaken;

	public Connections getConnections() {
		return connections;
	}

	public void setConnections(Connections connections) {
		this.connections = connections;
	}

	public String getAssociateId() {
		return associateId;
	}

	public void setAssociateId(String associateId) {
		this.associateId = associateId;
	}

	public long getConnectorNodeInitTaken() {
		return connectorNodeInitTaken;
	}

	public void setConnectorNodeInitTaken(long connectorNodeInitTaken) {
		this.connectorNodeInitTaken = connectorNodeInitTaken;
	}
}
