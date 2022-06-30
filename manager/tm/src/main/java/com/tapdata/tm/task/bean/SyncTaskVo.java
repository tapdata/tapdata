package com.tapdata.tm.task.bean;


import lombok.Data;

import java.util.Date;

/**
 * @Author: Zed
 * @Date: 2022/2/18
 * @Description:
 */
@Data
public class SyncTaskVo {


    private String id;

    private String name;
    /**
     * 源库时间点
     */
    private Date sourceTimestamp;
    /**
     * 增量同步时间点
     */
    private Date syncTimestamp;

    //private long delayTime;

    private String status;

    private String errorMessage;

    private String parentId;
    private String syncType;

}
