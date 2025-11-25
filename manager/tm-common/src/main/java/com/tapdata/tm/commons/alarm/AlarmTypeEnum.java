package com.tapdata.tm.commons.alarm;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public enum AlarmTypeEnum {
    SYNCHRONIZATIONTASK_ALARM("TASK","同步任务告警"),
    SHARED_CACHE_ALARM("TASK","共享缓存告警"),
    SHARED_MINING_ALARM("TASK","共享挖掘告警"),
    DATA_VERIFICATION_ALARM("TASK","数据校验告警"),
    ACCURATE_DELAY_ALARM("TASK","精准延迟告警"),
	INSPECT_ALARM("TASK","校验任务出错"),
    API_SERVER_ALARM("API_SERVER","API Server Alarm"),
    DATASOURCE_MONITOR_ALARM("CONNECTION", "Datasource Monitor Alarm");


    private final String type;
    private final String desc;

    AlarmTypeEnum(String type, String desc) {
        this.type = type;
        this.desc = desc;
    }

    public static List<String> get(String type) {
        List<String> alarmTypeEnums = new ArrayList<>();
        for (AlarmTypeEnum value : values()) {
            if (value.getType().equals(type)) {
                alarmTypeEnums.add(value.name());
            }
        }
        return alarmTypeEnums;
    }

    public static final String TYPE_TASK = "TASK";
}
