package com.tapdata.entity.dataflow.validator;

/**
 * 数据校验涉及的常量
 * Created by xj
 * 2020-04-16 01:29
 **/
public class ValidatorConstant {

	//校验状态
	public final static String STATUS_WAITING = "waiting";
	public final static String STATUS_VALIDATING = "validating";
	public final static String STATUS_COMPLETED = "completed";
	public final static String STATUS_DRAFT = "draft";
	public final static String STATUS_ERROR = "error";
	public final static String STATUS_INTERRUPTED = "interrupted";

	//校验类型
	public final static String VALIDATE_TYPE_ROW = "row";
	public final static String VALIDATE_TYPE_HASH = "hash";
	public final static String VALIDATE_TYPE_ADVANCE = "advance";

	//采样类型
	public final static String SAMPLING_ROWS = "rows";
	public final static String SAMPLING_RATE = "sampleRate";

	//校验结果类型
	public final static String RESULT_OVERVIEW = "overview";
	public final static String RESULT_TABLE_OVERVIEW = "tableOverview";
	public final static String RESULT_FAILED_ROW = "failedRow";
}
