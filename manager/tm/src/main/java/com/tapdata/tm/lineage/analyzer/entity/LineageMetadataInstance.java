package com.tapdata.tm.lineage.analyzer.entity;

import com.tapdata.tm.commons.schema.Field;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-05-25 12:09
 **/
@Data
public class LineageMetadataInstance {
	private String id;
	private String sourceType;
	private String nodeId;
	private List<Field> fields;
	private Map<String, Object> customProperties;
}
