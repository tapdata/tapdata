package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.Date;

@Data
public class TableStatus {
    /** 数据库id */
    private String srcNodeId;
    private String tgtNodeId;
    private String srcConnId;
    private String tgtConnId;
    private String srcName;
    private String tgtName;
    /** 表名 */
    private String srcTableName;
    private String tgtTableName;
    /** 总行数  源*/
    private Long totalNum;
    /** 成功同步的行数量  目标*/
    private Long finishNumber;
    /** 状态 前端去算 */
    private String status;
    /** 进度*/
    private Double progress;
    private Date startTs;
    //错误
    private String errorMsg;
}