package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.Date;

@Data
public class ShareCdcTableInfo {
    /** 表名*/
    private String name;
    /** 加入挖掘时间*/
    private Date joinTime;
    /** 首条日志时间*/
    private Date firstLogTime;
    /** 最新日志时间*/
    private String lastLogTime;
    /** 累计挖掘日志条数*/
    private String allCount;
    /** 进入挖掘日志条数*/
    private String todayCount;
}
