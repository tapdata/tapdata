package com.tapdata.entity;

import com.tapdata.entity.dataflow.Stage;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jackin
 */
public class JoinTable implements Serializable {

	private static final long serialVersionUID = -2093441683425945843L;

	private String joinType;

	private String joinPath;

	private String stageId;

	private boolean isArray;

	private List<Map<String, String>> joinKeys;

	/**
	 * many one  场景数组唯一键
	 */
	private String arrayUniqueKey;

	private boolean manyOneUpsert;

	public JoinTable() {
	}

	public JoinTable(JoinTable targetJoinTable, Stage stage, String joinType) {
		this.joinType = joinType;
		this.joinPath = null;
		this.stageId = stage.getId();

		List<Map<String, String>> joinKeys = new ArrayList<>();
		List<Map<String, String>> targetJoinKeys = targetJoinTable.getJoinKeys();
		if (CollectionUtils.isNotEmpty(targetJoinKeys)) {
			for (Map<String, String> targetJoinKey : targetJoinKeys) {
				Map<String, String> joinKey = new HashMap<>();
				joinKey.put("source", targetJoinKey.get("source"));
				joinKey.put("target", targetJoinKey.get("source"));
				joinKeys.add(joinKey);
			}
		}
		this.joinKeys = joinKeys;
	}

	public JoinTable(String joinType, String joinPath, String stageId, List<Map<String, String>> joinKeys) {
		this.joinType = joinType;
		this.joinPath = joinPath;
		this.stageId = stageId;
		this.joinKeys = joinKeys;
	}

	public JoinTable(Stage stage, String joinType) {
		this.joinType = joinType;
		this.joinPath = null;
		this.stageId = stage.getId();
		String primaryKeys = stage.getPrimaryKeys();
		String[] pks = primaryKeys.split(",");
		List<Map<String, String>> joinKeys = new ArrayList<>();
		for (String pk : pks) {
			Map<String, String> joinKey = new HashMap<>();
			joinKey.put("source", pk);
			joinKey.put("target", pk);
			joinKeys.add(joinKey);
		}
		this.joinKeys = joinKeys;
	}

	public boolean getManyOneUpsert() {
		return manyOneUpsert;
	}

	public void setManyOneUpsert(boolean manyOneUpsert) {
		this.manyOneUpsert = manyOneUpsert;
	}

	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public String getJoinPath() {
		return joinPath;
	}

	public void setJoinPath(String joinPath) {
		this.joinPath = joinPath;
	}

	public List<Map<String, String>> getJoinKeys() {
		return joinKeys;
	}

	public void setJoinKeys(List<Map<String, String>> joinKeys) {
		this.joinKeys = joinKeys;
	}

	public String getStageId() {
		return stageId;
	}

	public boolean getIsArray() {
		return isArray;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
	}

	public void setIsArray(boolean isArray) {
		this.isArray = isArray;
	}

	public String getArrayUniqueKey() {
		return arrayUniqueKey;
	}

	public void setArrayUniqueKey(String arrayUniqueKey) {
		this.arrayUniqueKey = arrayUniqueKey;
	}
}
