package com.tapdata.entity.dataflow.validator;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 校验概览数据
 * Created by xj
 * 2020-04-16 01:26
 **/
@Data
public class ValidatorResultTableOverview implements Serializable {

	private static final long serialVersionUID = 4055218751485157384L;

	private int id;

	private Long validateTime; //执行校验时间

	private String dataFlowId; //该记录所属的dataFlow ID

	private List<ValidatorResultTable> validateStats; //表校验结果列表

}
