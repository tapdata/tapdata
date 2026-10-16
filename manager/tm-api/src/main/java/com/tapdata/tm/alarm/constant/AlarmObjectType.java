package com.tapdata.tm.alarm.constant;

import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import org.apache.commons.lang3.StringUtils;

/**
 * 告警对象类型，由告警记录已有的标识推导，不新增存储字段。
 */
public enum AlarmObjectType {
    TASK,
    NODE,
    CONNECTION,
    AGENT,
    INSPECT,
    API_SERVER;

    public static AlarmObjectType of(AlarmInfo info) {
        if (info == null) {
            return null;
        }
        if (StringUtils.isNotBlank(info.getInspectId())) {
            return INSPECT;
        }
        if (AlarmTypeEnum.API_SERVER_ALARM.equals(info.getType()) || StringUtils.isNotBlank(info.getServerId())
                || StringUtils.isNotBlank(info.getApiId())) {
            return API_SERVER;
        }
        if (AlarmTypeEnum.DATASOURCE_MONITOR_ALARM.equals(info.getType())
                || (StringUtils.isBlank(info.getTaskId()) && StringUtils.isNotBlank(info.getConnectionId()))) {
            return CONNECTION;
        }
        if (StringUtils.isNotBlank(info.getTaskId())) {
            return StringUtils.isNotBlank(info.getNodeId()) ? NODE : TASK;
        }
        return AGENT;
    }
}
