package io.tapdata.connector.mysql.entity;

import java.io.Serializable;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-05-05 14:48
 **/
public class MysqlStreamOffset implements Serializable {

	private static final long serialVersionUID = 7107575040120294790L;
	private String name;
	private Map<String, String> offset;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, String> getOffset() {
		return offset;
	}

	public void setOffset(Map<String, String> offset) {
		this.offset = offset;
	}

	@Override
	public String toString() {
		return "MysqlStreamOffset{" +
				"name='" + name + '\'' +
				", offset=" + offset +
				'}';
	}
}
