package com.tapdata.tm.lineagegraph.service;

import com.harium.graph.Graph;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

/**
 * @Author: Zed
 * @Date: 2021/10/18
 * @Description:
 */
@Component
public class LineageTable {

    public Graph buildGraph(DataFlowDto dataFlow, Graph graph) {
        if (dataFlow == null || CollectionUtils.isEmpty(dataFlow.getStages()) || graph == null) {
            return null;
        }

        String mappingTemplate = dataFlow.getMappingTemplate();
        switch (mappingTemplate) {
            case "custom" :
                buildGraphFromCustomDataFlow(dataFlow, graph);
                break;
            case "cluster-clone":
                buildGraphFromCloneDataFlow(dataFlow, graph);
                break;
            default:
                break;
        }
        return graph;
    }

    private void buildGraphFromCloneDataFlow(DataFlowDto dataFlow, Graph graph) {

    }

    private void buildGraphFromCustomDataFlow(DataFlowDto dataFlow, Graph graph) {
    }
}
