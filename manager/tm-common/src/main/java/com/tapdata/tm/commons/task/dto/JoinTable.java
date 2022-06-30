/**
 * @title: JoinTable
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.commons.task.dto;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class JoinTable implements Serializable {

	private String joinType;

	private String joinPath;

	private String stageId;

	private Boolean isArray;

	private List<Map<String, String>> joinKeys;

	private String arrayUniqueKey;

	private Boolean manyOneUpsert;

	public String getJoinType() {
		return joinType;
	}

	public String getJoinPath() {
		return joinPath;
	}

	public String getStageId() {
		return stageId;
	}

	public Boolean getArray() {
		return isArray;
	}

	public List<Map<String, String>> getJoinKeys() {
		return joinKeys;
	}

	public String getArrayUniqueKey() {
		return arrayUniqueKey;
	}

	public Boolean getManyOneUpsert() {
		return manyOneUpsert;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public void setJoinPath(String joinPath) {
		this.joinPath = joinPath;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
	}

	public void setArray(Boolean array) {
		isArray = array;
	}

	public void setJoinKeys(List<Map<String, String>> joinKeys) {
		this.joinKeys = joinKeys;
	}

	public void setArrayUniqueKey(String arrayUniqueKey) {
		this.arrayUniqueKey = arrayUniqueKey;
	}

	public void setManyOneUpsert(Boolean manyOneUpsert) {
		this.manyOneUpsert = manyOneUpsert;
	}
}
