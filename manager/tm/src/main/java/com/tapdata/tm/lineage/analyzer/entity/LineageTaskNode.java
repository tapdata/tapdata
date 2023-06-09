package com.tapdata.tm.lineage.analyzer.entity;

import com.tapdata.tm.commons.dag.DAG;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-05-23 17:49
 **/
@Getter
@Setter
public class LineageTaskNode extends LineageNode {
	public static final String TASK_NODE_SOURCE_POS = "source";
	public static final String TASK_NODE_TARGET_POS = "target";
	private static final long serialVersionUID = 525507855376095221L;
	private String taskNodePos;

	public LineageTaskNode(String id, String name, String type) {
		super(id, type);
		setName(name);
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
