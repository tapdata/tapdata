package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class SampleTaskVo {
    /** 任务id */
    private String id;
    /** 任务名称 */
    private String name;
    /** 创建时间 */
    private Date createTime;
    /** 更新时间 */
    private Date lastUpdated;
    /** 状态 */
    private String status;
    private String syncStatus;
    /** 任务类型  数据复制还是数据开发 */
    private String syncType;
    /** 增量时间点 */
    private Long currentEventTimestamp;
    /** 增量延迟 */
    private Long delayTime;
    /** 源连接id */
    private List<String> sourceConnectionIds;
    /** 目标连接id */
    private List<String> targetConnectionId;
    /** 创建人 */
    private String createUser;
    /** 启动时间 */
    private Date startTime;
}
