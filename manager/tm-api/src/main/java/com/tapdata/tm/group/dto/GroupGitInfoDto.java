package com.tapdata.tm.group.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class GroupGitInfoDto extends BaseDto {
	private String repoUrl;
	private String token;
	private String branch = "main";
}
