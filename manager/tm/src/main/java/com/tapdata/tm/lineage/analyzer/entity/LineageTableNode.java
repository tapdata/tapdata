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
public class LineageTableNode extends LineageNode {
	public static final String NODE_TYPE = "tableLineage";
	private static final long serialVersionUID = 4749925482797754112L;
	private String table;
	private String connectionId;
	private String connectionName;
	private String pdkHash;
	private Map<String, LineageTask> tasks;

	public LineageTableNode(String table, String connectionId, String connectionName, String pdkHash) {
		super(LineageNode.genId(NODE_TYPE, connectionId, table), NODE_TYPE);
		this.table = table;
		this.connectionId = connectionId;
		this.connectionName = connectionName;
		this.pdkHash = pdkHash;
	}

	public synchronized LineageTableNode addTask(LineageTask lineageTask) {
		if (null == lineageTask) return this;
		if (null == tasks) {
			tasks = new HashMap<>();
		}
		tasks.put(lineageTask.getId(), lineageTask);
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
