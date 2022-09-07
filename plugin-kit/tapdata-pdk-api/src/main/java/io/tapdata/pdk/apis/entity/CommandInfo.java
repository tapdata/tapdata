package io.tapdata.pdk.apis.entity;

import java.util.Date;
import java.util.Map;

public class CommandInfo {
	private String pdkId;
	private String group;
	private String version;
	private Map<String, Object> connectionConfig;
	private Map<String, Object> nodeConfig;
	private String command;
	private String action;
	private Map<String, Object> argMap;
	private Long time;

	@Override
	public String toString() {
		return "CommandInfo pdkId " + pdkId + " group " + group + " version " + version + " command " + command + " action " + action + " time " + new Date(time) + " connectionConfig " + connectionConfig + " nodeConfig " + nodeConfig + ";";
	}

	public String getPdkId() {
		return pdkId;
	}

	public void setPdkId(String pdkId) {
		this.pdkId = pdkId;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
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
}
