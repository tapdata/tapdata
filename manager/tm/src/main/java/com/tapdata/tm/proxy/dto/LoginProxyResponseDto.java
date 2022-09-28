package com.tapdata.tm.proxy.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class LoginProxyResponseDto extends BaseDto {
	private String token;
	private Integer wsPort;
	private String wsPath;
}
