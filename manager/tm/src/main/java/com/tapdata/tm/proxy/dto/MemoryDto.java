package com.tapdata.tm.proxy.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class MemoryDto extends BaseDto {
	private String processId;
	private List<String> keys;
	private String keyRegex;
	private String level;
}
