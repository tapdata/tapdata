/**
 * @title: UpdataStatusRequest
 * @description:
 * @author lk
 * @date 2021/12/7
 */
package com.tapdata.tm.cluster.dto;

import javax.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class UpdataStatusRequest {

	@NotBlank
	private String uuid;

	@NotBlank
	private String server;

	@NotBlank
	private String operation;
}
