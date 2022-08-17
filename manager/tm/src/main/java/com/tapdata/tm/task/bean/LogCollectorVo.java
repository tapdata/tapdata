package com.tapdata.tm.task.bean;

import com.tapdata.tm.commons.task.dto.Status;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Date;
import java.util.List;

/**
 * @Author: Zed
 * @Date: 2022/2/17
 * @Description:
 */
@Data
public class LogCollectorVo {
    private String id;
    private String name;
    private List<Pair<String, String>> connections;
    private Date createTime;
    private String status;
    private List<Status> statuses;
    private List<String> tableName;
    /** //  current - 从浏览器当前时间
     //  localTZ - 从指定的时间开始(浏览器时区)
     //  connTZ - 从指定的时间开始(数据库时区)*/
    private String syncTimePoint = "current";
    /** 时区 */
    private String syncTimeZone;

    private Date syncTime;
    /** 保留的时长 单位 （天） 默认3天*/
    private Integer storageTime = 3;
}
