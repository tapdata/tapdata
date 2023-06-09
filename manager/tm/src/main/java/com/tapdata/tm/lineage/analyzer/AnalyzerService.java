package com.tapdata.tm.lineage.analyzer;

import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.lineage.entity.LineageType;
import io.github.openlg.graphlib.Graph;
import org.springframework.stereotype.Service;

/**
 * @author samuel
 * @Description
 * @create 2023-05-22 14:40
 **/
@Service
public interface AnalyzerService {
	Graph<Node, Edge> analyzeTable(String connectionId, String table, LineageType lineageType) throws Exception;
}
