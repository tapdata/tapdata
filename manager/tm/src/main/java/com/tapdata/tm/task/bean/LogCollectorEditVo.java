package com.tapdata.tm.task.bean;

import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.task.dto.TaskDto;
import lombok.Data;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2022/2/18
 * @Description:
 */
@Data
public class LogCollectorEditVo {
    private String id;

    private String name;
    /** 保留的时长 单位 （天） 默认3天*/
    private Integer storageTime;

    /** //  current - 从浏览器当前时间
     //  localTZ - 从指定的时间开始(浏览器时区)
     //  connTZ - 从指定的时间开始(数据库时区)*/
    private String syncTimePoint;
    /** 时区 */
    private String syncTineZone;

    private String syncTime;

    /** 增量时间点*/
    @EqField
    private List<TaskDto.SyncPoint> syncPoints;
}
