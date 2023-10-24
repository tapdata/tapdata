package io.tapdata.pdk.apis.entity.merge;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 多表合并原地更新模式调回产品（主从合并）  属性类
 */
public class MergeTableProperties implements Serializable {
	private static final long serialVersionUID = -7342251093319074592L;
	private MergeType mergeType;
	// 关联条件
	private List<Map<String, String>> joinKeys;
	// 内嵌数组关联条件
	private List<String> arrayKeys;
	private String tableName;
	private String targetPath;
	private boolean isArray;
	private String arrayPath;

	public String getArrayPath() {
		return arrayPath;
	}

	public void setArrayPath(String arrayPath) {
		this.arrayPath = arrayPath;
	}

	public MergeType getMergeType() {
		return mergeType;
	}

	public void setMergeType(MergeType mergeType) {
		this.mergeType = mergeType;
	}

	public List<Map<String, String>> getJoinKeys() {
		return joinKeys;
	}

	public void setJoinKeys(List<Map<String, String>> joinKeys) {
		this.joinKeys = joinKeys;
	}

	public List<String> getArrayKeys() {
		return arrayKeys;
	}

	public void setArrayKeys(List<String> arrayKeys) {
		this.arrayKeys = arrayKeys;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTargetPath() {
		return targetPath;
	}

	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}

	public boolean getIsArray() {
		return isArray;
	}

	public void setIsArray(boolean array) {
		isArray = array;
	}

	public enum MergeType {
		updateOrInsert(1), // 更新已存在或者插入新数据
		appendWrite(1),    // 追加写入
		updateWrite(2),    // 更新写入
		updateIntoArray(2), // 更新进内嵌数组
		;

		private int sort;

		MergeType(int sort) {
			this.sort = sort;
		}

		public int getSort() {
			return sort;
		}
	}
}
