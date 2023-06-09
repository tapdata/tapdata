package com.tapdata.tm.lineage.analyzer.entity;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author samuel
 * @Description
 * @create 2023-05-24 16:33
 **/
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class LineageModules extends LineageAttr {
	public static final String ATTR_KEY = "modules";
	private String datasource;
	private String table;
	private String basePath;
	private String status;
	private String appName;

	public LineageModules(String id, String name, String datasource, String table, String basePath, String status, String appName) {
		super(id, ATTR_KEY, name);
		this.datasource = datasource;
		this.table = table;
		this.basePath = basePath;
		this.status = status;
		this.appName = appName;
	}
}
