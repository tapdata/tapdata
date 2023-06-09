package com.tapdata.tm.commons.task.constant;

import lombok.Getter;

@Getter
public enum AlarmKeyEnum {
    TASK_STATUS_ERROR(Constant.TYPE_EVENT),
    TASK_INSPECT_ERROR(Constant.TYPE_EVENT),
    TASK_FULL_COMPLETE(Constant.TYPE_EVENT),
    TASK_INCREMENT_START(Constant.TYPE_EVENT),
    TASK_STATUS_STOP(Constant.TYPE_EVENT),
    TASK_INCREMENT_DELAY(Constant.TYPE_METRIC),
    DATANODE_CANNOT_CONNECT(Constant.TYPE_METRIC),
    DATANODE_HTTP_CONNECT_CONSUME(Constant.TYPE_METRIC),
    DATANODE_TCP_CONNECT_CONSUME(Constant.TYPE_METRIC),
    DATANODE_AVERAGE_HANDLE_CONSUME(Constant.TYPE_METRIC),
    PROCESSNODE_AVERAGE_HANDLE_CONSUME(Constant.TYPE_METRIC),

    SYSTEM_FLOW_EGINGE_DOWN(Constant.TYPE_METRIC),

    SYSTEM_FLOW_EGINGE_UP(Constant.TYPE_METRIC),
	INSPECT_TASK_ERROR(Constant.TYPE_EVENT),
	INSPECT_COUNT_ERROR(Constant.TYPE_EVENT),
	INSPECT_VALUE_ERROR(Constant.TYPE_EVENT),
	;


    private String type;

    AlarmKeyEnum(String type) {
        this.type = type;
    }

    public static class Constant {
        public static final String TYPE_EVENT = "event";
        public static final String TYPE_METRIC = "metric";
    }
}
