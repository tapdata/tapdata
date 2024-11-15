package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Collection;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-09-29 15:04
 **/
@NodeType("preview_target")
@Getter
@ToString
@Setter
public class PreviewTargetNode extends Node<Schema> {
	public PreviewTargetNode() {
		super(PreviewTargetNode.class.getAnnotation(NodeType.class).value());
	}

	@Override
	public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
		return null;
	}

	@Override
	protected Schema loadSchema(List<String> includes) {
		return null;
	}

	@Override
	protected Schema saveSchema(Collection<String> predecessors, String nodeId, Schema schema, DAG.Options options) {
		return null;
	}

	@Override
	protected Schema cloneSchema(Schema schema) {
		return null;
	}
}
