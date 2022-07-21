package com.tapdata.entity.dataflow.validator;

import lombok.Data;

import java.io.Serializable;

/**
 * 校验概览数据
 * Created by xj
 * 2020-04-16 01:26
 **/
@Data
public class ValidatorResultOverview implements Serializable {

	private static final long serialVersionUID = 4055218751485157384L;

	private int id;

	private Long validateTime; //执行校验时间

	private String dataFlowId; //该记录所属的dataFlow ID

	private int costTime; //校验耗时

	private int validateStats; //行数校验条数

	private int validateHashRows; //哈希校验条数

	private int validateAdvanceRows; //高级校验条数

	private int rowsDiffer; //总体行数差

	private int rowsMismatch; //不匹配条数

	private int consistencyRate; //一致率（0-100）

}
