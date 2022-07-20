package com.tapdata.entity;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * @author samuel
 * @Description
 * @create 2020-08-07 21:28
 **/
public class LogCollectInfo implements Serializable {

	private static final long serialVersionUID = 134002754199871532L;
	private Connections connections;
	private Set<String> includeTables;
	private String selectType;

	public LogCollectInfo(Connections connections, Set<String> includeTables) {
		this.connections = connections;
		this.includeTables = includeTables == null ? new HashSet<>() : includeTables;
	}

	public Connections getConnections() {
		return connections;
	}

	public void setConnections(Connections connections) {
		this.connections = connections;
	}

	public Set<String> getIncludeTables() {
		return includeTables;
	}

	public void setIncludeTables(Set<String> includeTables) {
		this.includeTables = includeTables;
	}

	public String getSelectType() {
		return selectType;
	}

	public void setSelectType(String selectType) {
		this.selectType = selectType;
	}
}
