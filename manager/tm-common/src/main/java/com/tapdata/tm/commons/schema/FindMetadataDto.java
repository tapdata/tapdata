package com.tapdata.tm.commons.schema;

import lombok.Data;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-10-31 18:41
 **/
@Data
public class FindMetadataDto {
	private String metaType;
	private List<String> tableNames;
}
