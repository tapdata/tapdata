package com.tapdata.tm.commons.customNode;

import lombok.Data;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-03-17 22:17
 **/
@Data
public class CustomNodeTempDto {
	private String name;
	private String desc;
	private String icon;
	private Map<String,Object> formSchema;
	private String template;
}
