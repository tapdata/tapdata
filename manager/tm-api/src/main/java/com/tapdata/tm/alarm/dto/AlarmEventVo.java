package com.tapdata.tm.alarm.dto;

import com.tapdata.tm.alarm.constant.AlarmObjectType;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.notification.delivery.dto.DeliverySummaryVo;
import lombok.Data;

import java.util.Date;

/**
 * 告警事件列表行与详情，详情另含当前值、阈值等字段。
 */
@Data
public class AlarmEventVo {
    private String id;
    private Level level;
    private AlarmStatusEnum status;
    private AlarmKeyEnum metric;
    private String summary;
    private AlarmObjectType objectType;
    private String objectId;
    /** 告警写入时保存的名称；对象删除后仍可展示 */
    private String objectName;
    private boolean objectDeleted;
    private String taskId;
    private String taskName;
    private String syncType;
    private String nodeId;
    private String nodeName;
    private String agentId;
    private String connectionId;
    private String inspectId;
    private String serverId;
    private String apiId;
    private Integer currentValue;
    private Integer threshold;
    private Date firstOccurrenceTime;
    private Date lastOccurrenceTime;
    private Integer occurrenceCount;
    private Date recoveryTime;
    private Date closeTime;
    private String closeBy;
    /** 任务重新运行时由系统归档 */
    private boolean archived;
    private Date lastNotifyTime;
    private DeliverySummaryVo deliverySummary;
}
