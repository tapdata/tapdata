package com.tapdata.tm.modules.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ModulesUpAndLoadDto {
	private String collectionName;
	private String json;
}
