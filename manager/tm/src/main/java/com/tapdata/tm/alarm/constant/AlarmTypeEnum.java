package com.tapdata.tm.alarm.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum AlarmTypeEnum {
    SYNCHRONIZATIONTASK_ALARM("同步任务告警"),
    SHARED_CACHE_ALARM("共享缓存告警"),
    SHARED_MINING_ALARM("共享挖掘告警"),
    DATA_VERIFICATION_ALARM("数据校验告警"),
    ACCURATE_DELAY_ALARM("精准延迟告警"),
		INSPECT_ALARM("校验任务出错");

    private final String desc;
}
