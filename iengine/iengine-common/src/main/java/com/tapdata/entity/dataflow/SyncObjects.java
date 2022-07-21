package com.tapdata.entity.dataflow;

import java.io.Serializable;
import java.util.List;

/**
 * @author jackin
 * @date 2020/9/8 5:32 PM
 **/
public class SyncObjects implements Serializable {

	private static final long serialVersionUID = -1271896748306573868L;

	public static final String TABLE_TYPE = "table";
	public static final String VIEW_TYPE = "view";
	public static final String FUNCTION_TYPE = "function";
	public static final String PROCEDURE_TYPE = "procedure";
	public static final String INDEX_TYPE = "index";
	public static final String QUEUE = "queue";
	public static final String TOPIC = "topic";

	private String type;

	private List<String> objectNames;

	private int sort;

	public SyncObjects() {
	}

	public SyncObjects(String type, List<String> objectNames) {
		this.type = type;
		this.objectNames = objectNames;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<String> getObjectNames() {
		return objectNames;
	}

	public void setObjectNames(List<String> objectNames) {
		this.objectNames = objectNames;
	}

	public int getSort() {
		return sort;
	}

	public void setSort(int sort) {
		this.sort = sort;
	}

	@Override
	public String toString() {
		return "SyncObjects{" +
				"type='" + type + '\'' +
				", objectNames=" + objectNames +
				", sort=" + sort +
				'}';
	}
}
