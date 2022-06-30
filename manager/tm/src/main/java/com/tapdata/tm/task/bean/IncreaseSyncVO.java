package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.Date;

/**
 * @Author: Zed
 * @Date: 2021/12/2
 * @Description: 增量同步返回实体
 */
@Data
public class IncreaseSyncVO {
    /** 源数据库id */
    private String srcId;
    /** 预案数据源名称*/
    private String srcConnId;
    private String srcName;
    /** 源表名称*/
    private String srcTableName;
    /** 目标数据源id */
    private String tgtId;
    private String tgtConnId;
    /** 目标数据源名称 */
    private String tgtName;
    /** 目标表名称*/
    private String tgtTableName;
    /** 延迟 */
    private Long delay;
    /** 当前执行到的的时间点 */
    private Date cdcTime;
}
