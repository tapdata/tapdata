package com.tapdata.tm.lineage.analyzer.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author samuel
 * @Description
 * @create 2023-05-24 16:53
 **/
@Data
@AllArgsConstructor
public class LineageAttr {
	private String id;
	private String attrKey;
	private String name;
}
