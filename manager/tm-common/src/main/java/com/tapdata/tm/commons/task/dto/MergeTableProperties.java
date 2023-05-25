package com.tapdata.tm.commons.task.dto;

import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 多表合并原地更新模式调回产品（主从合并）  属性类
 */
@Data
public class MergeTableProperties implements Serializable {
	private static final long serialVersionUID = -7342251093319074592L;
	private MergeType mergeType;
	// 关联条件
	private List<Map<String, String>> joinKeys;
	// 内嵌数组关联条件
	private List<String> arrayKeys;
	private String tableName;
	private String connectionId;
	//private String sourId;
	private String targetPath;
	private String id;
	List<MergeTableProperties> children;
	private boolean isArray;
	private String arrayPath;

	public boolean getIsArray() {
		return isArray;
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


	public static void autoFillingArray(MergeTableProperties properties, boolean parentUpdateArray) {
		properties.setArray(parentUpdateArray);
		List<MergeTableProperties> children = properties.getChildren();
		if (CollectionUtils.isNotEmpty(children)) {
			for (MergeTableProperties child : children) {
				autoFillingArray(child, properties.getMergeType().equals(MergeType.updateIntoArray));
			}
		}
	}

}
