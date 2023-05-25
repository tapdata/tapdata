package com.tapdata.tm.lineage.analyzer.entity;

import com.tapdata.tm.commons.dag.DAG;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-05-22 16:18
 **/
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class LineageModuleNode extends LineageNode {
	public static final String NODE_TYPE = "apiserverLineage";
	private static final long serialVersionUID = 4749925482797754112L;
	private String datasource;
	private String table;
	private Map<String, LineageModules> modules;

	public LineageModuleNode(String table, String datasource, LineageModules lineageModules) {
		super(genId(NODE_TYPE, lineageModules.getId(), datasource, table), NODE_TYPE);
		this.table = table;
		this.datasource = datasource;
		addModule(lineageModules);
	}

	public synchronized LineageModuleNode addModule(LineageModules lineageModules) {
		if (null == lineageModules) return this;
		if (null == modules) {
			modules = new HashMap<>();
		}
		modules.put(lineageModules.getId(), lineageModules);
		return this;
	}


	@Override
	public Object mergeSchema(List inputSchemas, Object o, DAG.Options options) {
		return null;
	}

	@Override
	protected Object cloneSchema(Object o) {
		return null;
	}

	@Override
	protected Object saveSchema(Collection predecessors, String nodeId, Object schema, DAG.Options options) {
		return null;
	}

	@Override
	protected Object loadSchema(List includes) {
		return null;
	}
}
