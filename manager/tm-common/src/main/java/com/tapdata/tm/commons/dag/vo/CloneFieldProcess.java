/**
 * @title: CloneFieldProcess
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.commons.dag.vo;

import com.tapdata.tm.commons.dag.process.FieldProcessorNode;

import java.io.Serializable;
import java.util.List;

public class CloneFieldProcess implements Serializable {

	private String table_id;

	private String table_name;

	private List<FieldProcessorNode.Operation> operations;

	public String getTable_id() {
		return table_id;
	}

	public String getTable_name() {
		return table_name;
	}

	public List<FieldProcessorNode.Operation> getOperations() {
		return operations;
	}

	public void setTable_id(String table_id) {
		this.table_id = table_id;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public void setOperations(List<FieldProcessorNode.Operation> operations) {
		this.operations = operations;
	}
}
