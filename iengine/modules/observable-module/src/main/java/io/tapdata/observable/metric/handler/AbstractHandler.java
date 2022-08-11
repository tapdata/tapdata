package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.TaskDto;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Dexter
 */
abstract class AbstractHandler {
    static final String SAMPLE_TYPE_ENGINE = "engine";
    static final String SAMPLE_TYPE_TASK = "task";
    static final String SAMPLE_TYPE_NODE = "node";
    static final String SAMPLE_TYPE_TABLE = "table";

    final TaskDto task;

    AbstractHandler(TaskDto task) {
        this.task = task;
    }

    Map<String, String> baseTags(String type) {
        return new HashMap<String, String>() {{
            put("type", type);
            put("taskId", task.getId().toHexString());
        }};
    }

    TaskDto getTask() {
        return task;
    }
}
