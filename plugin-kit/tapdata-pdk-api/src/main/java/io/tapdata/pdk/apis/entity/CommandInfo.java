package io.tapdata.pdk.apis.entity;

import java.util.Date;
import java.util.Map;

public class CommandInfo {
	private String id;
	private String pdkHash;
	private Map<String, Object> connectionConfig;
	private Map<String, Object> nodeConfig;
	private String command;
	private String action;
	private Map<String, Object> argMap;
	private Long time;

	@Override
	public String toString() {
		return "CommandInfo id " + id + " pdkHash " + pdkHash + " command " + command + " action " + action + " time " + (time != null ? new Date(time) : null) + " connectionConfig " + connectionConfig + " nodeConfig " + nodeConfig + ";";
	}

	public String getPdkHash() {
		return pdkHash;
	}

	public void setPdkHash(String pdkHash) {
		this.pdkHash = pdkHash;
	}

	public Map<String, Object> getConnectionConfig() {
		return connectionConfig;
	}

	public void setConnectionConfig(Map<String, Object> connectionConfig) {
		this.connectionConfig = connectionConfig;
	}

	public Map<String, Object> getNodeConfig() {
		return nodeConfig;
	}

	public void setNodeConfig(Map<String, Object> nodeConfig) {
		this.nodeConfig = nodeConfig;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public Map<String, Object> getArgMap() {
		return argMap;
	}

	public void setArgMap(Map<String, Object> argMap) {
		this.argMap = argMap;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
