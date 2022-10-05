package com.tapdata.tm.proxy.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class SubscribeResponseDto extends BaseDto {
	private String token;
}
