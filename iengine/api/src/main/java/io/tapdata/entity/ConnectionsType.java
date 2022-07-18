package io.tapdata.entity;

public enum ConnectionsType {
	SOURCE("source"),
	TARGET("target"),
	SOURCEANDTARGET("source_and_target"),
	;

	private String type;

	ConnectionsType(String type) {
		this.type = type;
	}

	public static ConnectionsType getConnectionType(String type) {
		for (ConnectionsType connectionsType : ConnectionsType.values()) {
			if (connectionsType.type.equals(type)) {
				return connectionsType;
			}
		}
		return null;
	}

	public String getType() {
		return type;
	}
}
