package com.tapdata.tm.alarm.dto;

import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import lombok.Data;

import java.util.List;

/** Draft receiver values used to preview an Inspect task before it is saved. */
@Data
public class InspectReceiverPreviewRequest {
    private String inspectId;
    private String flowId;
    private List<AlarmReceiver> alarmReceivers;
    private List<String> emailReceivers;
}
