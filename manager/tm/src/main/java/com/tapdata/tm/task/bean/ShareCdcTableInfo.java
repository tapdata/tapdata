package com.tapdata.tm.task.bean;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Data
public class ShareCdcTableInfo {
    /** 表名*/
    private String name;
    /** 连接名称 */
    private String connectionName;

    private String connectionId;
    /** 加入挖掘时间*/
    private Date joinTime;
    /** 首条日志时间*/
    private Date firstLogTime;
    /** 最新日志时间*/
    private Date lastLogTime;
    /** 累计挖掘日志条数*/
    private BigDecimal allCount;
    /** 进入挖掘日志条数*/
    private Long todayCount;
}
