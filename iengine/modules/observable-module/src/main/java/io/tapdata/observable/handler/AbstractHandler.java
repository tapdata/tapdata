package io.tapdata.observable.handler;

import com.tapdata.tm.commons.task.dto.SubTaskDto;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Dexter
 */
abstract class AbstractHandler {
    static final String SAMPLE_TYPE_TASK = "task";
    static final String SAMPLE_TYPE_NODE = "node";
    static final String SAMPLE_TYPE_TABLE = "table";

    final SubTaskDto task;

    AbstractHandler(SubTaskDto task) {
        this.task = task;
    }

    Map<String, String> baseTags(String type) {
        return new HashMap<String, String>() {{
            put("type", type);
            put("taskId", task.getParentTask().getId().toHexString());
            put("subTaskId", task.getId().toHexString());
        }};
    }

    SubTaskDto getTask() {
        return task;
    }
}
