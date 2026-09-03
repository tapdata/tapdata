package com.tapdata.tm.commons.task.dto.alarm;

import lombok.Data;

/**
 * Engine-to-TM payload for {@code Log.alert()}. TM loads task/agent context itself.
 */
@Data
public class TaskAlertRequest {
    private String taskId;
    private String nodeId;
    private String nodeName;
    private String message;
}
