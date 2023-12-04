package com.tapdata.entity.values;

import lombok.Data;

@Data
public class CheckEngineValidResultDto {
	/**
	 * 校验Engine结果
	 * true 校验通过
	 * false 校验未通过
	 */
	private Boolean result;
	/**
	 * Engine processId
	 */
	private String processId;
	/**
	 * 校验Engine未通过原因
	 */
	private String failedReason;
}
