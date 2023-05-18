package com.tapdata.tm.task.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShareCdcConnectionInfo {
    /** 连接id*/
    private String id;
    /** 连接名称*/
    private String name;
    /** 连接表详情*/
    private List<ShareCdcTableInfo> tableInfos;

    public ShareCdcConnectionInfo(String id, String name) {
        this.id = id;
        this.name = name;
    }
}
