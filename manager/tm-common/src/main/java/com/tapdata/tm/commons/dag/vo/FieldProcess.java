/**
 * @title: FieldProcess
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.commons.dag.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class FieldProcess implements Serializable {

	private List<FieldProcessorNode.Operation> operations;
	@JsonProperty("table_name")
	private String tableName;
	@JsonProperty("tableId")
	private String tableId;

}
