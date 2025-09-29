package com.tapdata.tm.commons.task.constant;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public enum AlarmKeyEnum {
    TASK_STATUS_ERROR(Constant.TYPE_EVENT),
    TASK_INSPECT_ERROR(Constant.TYPE_EVENT),
    TASK_FULL_COMPLETE(Constant.TYPE_EVENT),
    TASK_INCREMENT_START(Constant.TYPE_EVENT),
    TASK_STATUS_STOP(Constant.TYPE_EVENT),
    TASK_INCREMENT_DELAY(Constant.TYPE_METRIC),
    TASK_INSPECT_DIFFERENCE(Constant.TYPE_METRIC), // 任务内校验-差异告警
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
    TASK_RETRY_WARN(Constant.TYPE_METRIC),

    API_SERVER_WORKER_DELAY_P50_WARN(Constant.TYPE_API_SERVER, "任一Worker每分钟请求延迟 P50 >= ${delay}ms 并持续${min}分钟时邮件告警", 0),
    API_SERVER_WORKER_DELAY_P95_WARN(Constant.TYPE_API_SERVER, "任一Worker每分钟请求延迟 P95 >= ${delay}ms 并持续${min}分钟时系统通知", 10),
    API_SERVER_WORKER_DELAY_P99_WARN(Constant.TYPE_API_SERVER, "任一Worker每分钟请求延迟 P99 >= ${delay}ms 并持续${min}分钟时系统通知", 20),
    API_SERVER_WORKER_ERROR_RATE_WARN(Constant.TYPE_API_SERVER, "任一Worker每分钟请求错误率 >= ${rate}% 并持续${min}分钟时邮件告警", 30),
    API_SERVER_WORKER_ERROR_RATE_ALTER(Constant.TYPE_API_SERVER, "任一Worker每分钟请求错误率 >= ${rate}% 并持续${min}分钟时系统通知", 40),
    API_SERVER_API_DELAY_AVG_WARN(Constant.TYPE_API_SERVER, "任一API响应时间 >= ${rate}s 并持续${min}分钟时邮件告警", 50),
    API_SERVER_API_DELAY_P95_ALTER(Constant.TYPE_API_SERVER, "任一API每分钟请求延迟P95 >= ${delay}ms 并持续${min}分钟时系统通知", 60),
    API_SERVER_API_DELAY_P99_ALTER(Constant.TYPE_API_SERVER, "任一API每分钟请求延迟P99 >= ${delay}ms 并持续${min}分钟时系统通知", 70),
    API_SERVER_API_ERROR_RATE_ALTER(Constant.TYPE_API_SERVER, "任一API每分钟请求错误率 >= ${rate}% 并持续${min}分钟时系统通知", 80),
    API_SERVER_ALL_API_ERROR_RATE_ALTER(Constant.TYPE_API_SERVER, "所有请求每分钟错误率 >= ${rate}% 并持续${min}分钟时系统通知", 90),
    API_SERVER_API_RESPONSE_SIZE_ALTER(Constant.TYPE_API_SERVER, "任一请求返回数据大小 >= ${memory}MB 时系统通知", 100),
    API_SERVER_CPU_USAGE_WARN(Constant.TYPE_API_SERVER, "API Server CPU使用 >= ${rate1}% 时邮件告警", 110),
    API_SERVER_CPU_USAGE_ALTER(Constant.TYPE_API_SERVER, "API Server CPU使用 >= ${rate2}% 时系统通知", 120),
    API_SERVER_MEMORY_USAGE_WARN(Constant.TYPE_API_SERVER, "API Server内存使用 >= ${memory1}% 时邮件告警", 130),
    API_SERVER_MEMORY_USAGE_ALTER(Constant.TYPE_API_SERVER, "API Server内存使用 >= ${memory2}% 时系统通知", 140),
    API_SERVER_WORKER_CPU_USAGE_WARN(Constant.TYPE_API_SERVER, "任一Worker CPU使用 >= ${rate1}% 时邮件告警", 150),
    API_SERVER_WORKER_CPU_USAGE_ALTER(Constant.TYPE_API_SERVER, "任一Worker CPU使用 >= ${rate2}% 时系统通知", 160),
    API_SERVER_WORKER_MEMORY_USAGE_WARN(Constant.TYPE_API_SERVER, "任一Worker CPU使用 >= ${rate1}% 时邮件告警", 170),
    API_SERVER_WORKER_MEMORY_USAGE_ALTER(Constant.TYPE_API_SERVER, "任一Worker CPU使用 >= ${rate2}% 时系统通知", 180),

    ;


    private final String type;
    private final String description;
    private final int sort;

    AlarmKeyEnum(String type) {
        this(type, "", 0);
    }

    AlarmKeyEnum(String type, String description, int sort) {
        this.type = type;
        this.description = description;
        this.sort = sort;
    }

    public static class Constant {
        private Constant() {}
        public static final String TYPE_EVENT = "event";
        public static final String TYPE_METRIC = "metric";
        public static final String TYPE_API_SERVER = "api-server";
    }

    public static List<String> getTaskAlarmKeys() {
        List<String> keys = new ArrayList<>();
        for (AlarmKeyEnum value : AlarmKeyEnum.values()) {
            keys.add(value.name());
        }
        return keys;
    }
}
