package io.tapdata.connector.mysql.entity;

import java.io.Serializable;
import java.util.HashMap;
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

	private Map<String, Map<String,String>> offsetMap = new HashMap<>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		offsetMap.put(name, new HashMap<String,String>());
		this.name = name;
	}

	public Map<String, String> getOffset() {
		return offset;
	}

	public Map<String, Map<String,String>> getOffsetMap(){
		return offsetMap;
	}

	public void setOffset(Map<String, String> offset) {
		this.offset = offset;
	}

	public void setOffsetMap(String name, Map<String, String> offsetMap){
		this.offsetMap.put(name, offsetMap);
	}

	public Map<String, String> getOffset(String setName) {
		return offsetMap.get(setName);
	}

	@Override
	public String toString() {
		return "MysqlStreamOffset{" +
				"name='" + name + '\'' +
				", offset=" + offset +
				'}';
	}
}
