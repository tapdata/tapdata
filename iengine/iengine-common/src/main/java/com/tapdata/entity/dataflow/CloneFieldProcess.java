package com.tapdata.entity.dataflow;

import com.tapdata.entity.FieldProcess;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2021-06-05 14:35
 **/
public class CloneFieldProcess implements Serializable {

	private static final long serialVersionUID = 2512654459435523074L;

	private String table_id;

	private String table_name;

	private List<FieldProcess> operations;

	public CloneFieldProcess() {
	}

	public String getTable_id() {
		return table_id;
	}

	public void setTable_id(String table_id) {
		this.table_id = table_id;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public List<FieldProcess> getOperations() {
		return operations;
	}

	public void setOperations(List<FieldProcess> operations) {
		this.operations = operations;
	}

	private boolean isEmpty() {
		return StringUtils.isNoneBlank(table_id, table_name)
				&& CollectionUtils.isNotEmpty(operations);
	}
}
