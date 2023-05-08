package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.List;

@Data
public class ShareCdcConnectionInfo {
    /** 连接id*/
    private String id;
    /** 连接名称*/
    private String name;
    /** 连接表详情*/
    private List<ShareCdcTableInfo> tableInfos;
}
