package io.tapdata.pdk.core.utils;

import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.ProcessorNode;

public class LoggerUtils {
    private LoggerUtils() {}
    public static String nodePrefix(String dagId, String associateId, String pdkIdGroup, String pdkVersion) {
        return "dagId " + dagId + " associateId " + associateId + " pkdIdGroup " + pdkIdGroup + " version " + pdkVersion;
    }

    public static String sourceNodeMessage(ConnectorNode sourceNode) {
        return "Source " + nodePrefix(sourceNode.getDagId(), sourceNode.getAssociateId(), sourceNode.getTapNodeInfo().getTapNodeSpecification().idAndGroup(), sourceNode.getTapNodeInfo().getTapNodeSpecification().getVersion());
    }

    public static String targetNodeMessage(ConnectorNode targetNode) {
        return "Target " + nodePrefix(targetNode.getDagId(), targetNode.getAssociateId(), targetNode.getTapNodeInfo().getTapNodeSpecification().idAndGroup(), targetNode.getTapNodeInfo().getTapNodeSpecification().getVersion());
    }

    public static String processorNodeMessage(ProcessorNode processorNode) {
        return "Processor " + nodePrefix(processorNode.getDagId(), processorNode.getAssociateId(), processorNode.getTapNodeInfo().getTapNodeSpecification().idAndGroup(), processorNode.getTapNodeInfo().getTapNodeSpecification().getVersion());
    }
}
