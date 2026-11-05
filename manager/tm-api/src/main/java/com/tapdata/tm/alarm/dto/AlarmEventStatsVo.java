package com.tapdata.tm.alarm.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlarmEventStatsVo {
    /** 告警中 */
    private long alarming;
    /** 告警中且级别为严重或紧急 */
    private long severe;
    private long todayNew;
    private long todayRecovered;
}
