package com.tapdata.entity.dataflow.validator;

import com.tapdata.entity.dataflow.Stage;
import lombok.Data;

import java.io.Serializable;

/**
 * 校验概览数据
 * Created by xj
 * 2020-04-16 01:26
 **/
@Data
public class ValidatorResultFailedRow implements Serializable {

	private static final long serialVersionUID = 4055218751485157384L;

	private int id;

	private Long validateTime; //执行校验时间

	private String dataFlowId; //该记录所属的dataFlow ID

	private String validateType; // 用于下来菜单过滤

	private Stage sourceStage; // 源表信息

	private Stage targetStage; // 目标表信息

	private String sourceTableData; // 该条记录在源表的数据toString

	private String targetTableData; // 该条记录在目标表的数据toString

	private String message; // 有差别的数据字段信息

}
