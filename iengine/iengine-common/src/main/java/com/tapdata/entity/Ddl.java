package com.tapdata.entity;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2020-09-08 18:28
 **/
public class Ddl implements Serializable {

	private static final long serialVersionUID = -4118431686146822046L;
	private String name;
	private OperationType operationType;
	private String ddl;

	public Ddl() {
	}

	public Ddl(String name, OperationType operationType, String ddl) {
		this.name = name;
		this.operationType = operationType;
		this.ddl = ddl;
	}

	public String getName() {
		return name;
	}

	public OperationType getOperationType() {
		return operationType;
	}

	public String getDdl() {
		return ddl;
	}

	@Override
	public String toString() {
		return "Ddl{" +
				"name='" + name + '\'' +
				", operationType=" + operationType +
				", ddl='" + ddl + '\'' +
				'}';
	}

	public boolean isEmpty() {
		return StringUtils.isAnyBlank(name, ddl)
				|| operationType == null;
	}
}
