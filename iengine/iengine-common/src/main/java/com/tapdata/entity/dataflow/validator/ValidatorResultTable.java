package com.tapdata.entity.dataflow.validator;

import com.tapdata.entity.dataflow.Stage;
import lombok.Data;

/**
 * 校验概览数据
 * Created by xj
 * 2020-04-16 01:26
 **/
@Data
public class ValidatorResultTable {

	private Stage sourceStage;//校验源表信息

	private Stage targetStage;//校验目标表信息

	private String validateType; // row: 行数  hash：哈希  advance：高级校验

	private int costTime; //校验耗时

	private int validateRows; //行数校验条数

	private int validateHashRows; //哈希校验条数

	private int validateAdvanceRows; //高级校验条数

	private int rowsDiffer; //总体行数差

	private int rowsMismatch; //不匹配条数

	private int consistencyRate; //一致率（0-100）

}
