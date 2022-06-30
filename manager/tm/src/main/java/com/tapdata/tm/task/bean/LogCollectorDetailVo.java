package com.tapdata.tm.task.bean;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;

/**
 * @Author: Zed
 * @Date: 2022/2/18
 * @Description:
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class LogCollectorDetailVo extends LogCollectorVo {

    private List<SyncTaskVo> taskList;

    /** 挖掘时间 */
    private Date logTime;

    private long delayTime;
    private String subTaskId;
}
