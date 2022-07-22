package io.tapdata.observable.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.SubTaskDto;

import java.util.Map;

/**
 * @author Dexter
 */
abstract class AbstractNodeSampleHandler extends AbstractHandler {
    AbstractNodeSampleHandler(SubTaskDto task) {
        super(task);
    }

    public Map<String, String> nodeTags(Node<?> node) {
        Map<String, String> tags = baseTags(SAMPLE_TYPE_NODE);
        tags.put("nodeId", node.getId());
        tags.put("nodeType", node.getType());

        return tags;
    }
}
