package com.tapdata.tm.proxy.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class LoginProxyDto extends BaseDto {
	private String clientId;
	private String service;
	private Integer terminal;
}
