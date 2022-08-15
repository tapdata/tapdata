package com.tapdata.tm.discovery.bean;

import lombok.Data;

@Data
public class DirectoryQueryParam {
    /** directoryId */
    private String id;
    /** 来源类型 */
    private String sourceType;
    /**  */
    private Integer page = 1;
    /**  */
    private Integer pageSize = 20;
    /** 模糊查询key */
    private String queryKey;
}
