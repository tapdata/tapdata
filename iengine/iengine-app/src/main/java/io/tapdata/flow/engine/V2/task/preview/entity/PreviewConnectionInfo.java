package io.tapdata.flow.engine.V2.task.preview.entity;

import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;

/**
 * @author samuel
 * @Description
 * @create 2024-11-11 20:05
 **/
public class PreviewConnectionInfo {
	private String associateId;
	private Connections connections;
	private DatabaseTypeEnum.DatabaseType databaseType;

	public String getAssociateId() {
		return associateId;
	}

	public void setAssociateId(String associateId) {
		this.associateId = associateId;
	}

	public Connections getConnections() {
		return connections;
	}

	public void setConnections(Connections connections) {
		this.connections = connections;
	}

	public DatabaseTypeEnum.DatabaseType getDatabaseType() {
		return databaseType;
	}

	public void setDatabaseType(DatabaseTypeEnum.DatabaseType databaseType) {
		this.databaseType = databaseType;
	}
}
