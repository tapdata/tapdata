package com.tapdata.tm.lineage.analyzer;

import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.lineage.analyzer.entity.LineageTask;
import com.tapdata.tm.lineage.entity.LineageType;
import io.github.openlg.graphlib.Graph;
import lombok.Data;

import java.util.Set;

/**
 * @author samuel
 * @Description
 * @create 2023-05-19 18:41
 **/
@Data
public class AnalyzeLayer {
	private String connectionId;
	private String table;
	private LineageType lineageType;
	private LineageTask currentTask;
	private Graph<Node, Edge> graph;
	private Set<String> notInTaskIds;
	private Node preNode;
	private LineageTableNode preLineageTableNode;
	private Node preInvalidNode;
}
