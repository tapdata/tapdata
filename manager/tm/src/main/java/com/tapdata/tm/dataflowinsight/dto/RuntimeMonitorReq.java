/**
 * @title: RuntimeMonitorReq
 * @description:
 * @author lk
 * @date 2021/10/29
 */
package com.tapdata.tm.dataflowinsight.dto;

import javax.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RuntimeMonitorReq {

	@NotBlank
	private String statsType;

	@NotBlank
	private String granularity;

	@NotBlank
	private String dataFlowId;

	private String stageId;

}
