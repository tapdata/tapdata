/**
 * @title: SyncObjects
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.commons.dag.vo;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

public class SyncObjects implements Serializable {

	/**
	 * typle=allTables 复制/迁移所有的表
	 */
	private String type;

	private List<String> objectNames;

	private LinkedHashMap<String, String> tableNameRelation;

	private Integer sort;

	public String getType() {
		return type;
	}

	public List<String> getObjectNames() {
		return objectNames;
	}

	public Integer getSort() {
		return sort;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setObjectNames(List<String> objectNames) {
		this.objectNames = objectNames;
	}

	public void setSort(Integer sort) {
		this.sort = sort;
	}

	public LinkedHashMap<String, String> getTableNameRelation() {
		return tableNameRelation;
	}

	public void setTableNameRelation(LinkedHashMap<String, String> tableNameRelation) {
		this.tableNameRelation = tableNameRelation;
	}
}
