package com.tapdata.tm.lineagegraph.service;

import com.harium.graph.Graph;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2021/10/18
 * @Description:
 */
public class GraphUtil {

    public static Graph createGraph(Boolean directed, Boolean multigraph, Boolean compound) {
        if (directed == null) {
            directed = true;
        }

        if (multigraph == null) {
            multigraph = false;
        }

        if (compound == null) {
            compound = false;
        }
        Graph<String> graph = new Graph<>();
        return graph;
    }


    public static Graph getGraphByNode(Graph originalGraph, String graphId, MetadataInstancesDto metadata) {
        return getGraphByNode(originalGraph, graphId, metadata, null);
    }

    public static Graph getGraphByNode(Graph originalGraph, String graphId, MetadataInstancesDto metadata, List<String> fieldGraphIds) {
        if (graphId != null || CollectionUtils.isNotEmpty(originalGraph.getNodes())) {

        }
        return originalGraph;
    }


}
