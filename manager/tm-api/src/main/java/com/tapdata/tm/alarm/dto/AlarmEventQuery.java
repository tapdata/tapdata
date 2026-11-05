package com.tapdata.tm.alarm.dto;

import com.tapdata.tm.alarm.constant.AlarmObjectType;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import lombok.Data;

import java.util.List;

/**
 * 告警事件列表筛选条件，均可为空。
 */
@Data
public class AlarmEventQuery {
    private List<AlarmStatusEnum> status;
    private List<Level> level;
    private List<AlarmObjectType> objectType;
    private List<AlarmKeyEnum> metric;
    /** 对象名称或 ID */
    private String keyword;
    private String agentId;
    /** 最近发生时间范围（毫秒） */
    private Long start;
    private Long end;
    /** NONE / NOTIFIED / PARTIAL_FAILED / FAILED */
    private String notified;
    private Integer page = 1;
    private Integer size = 20;
}
