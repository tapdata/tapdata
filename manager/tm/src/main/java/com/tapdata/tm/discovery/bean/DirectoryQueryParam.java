package com.tapdata.tm.discovery.bean;

import lombok.Data;

@Data
public class DirectoryQueryParam {
    /** directoryId */
    private String id;
    /** 来源类型 */
    private String sourceType;
    /**  */
    private Integer skip = 0;
    /**  */
    private Integer size = 20;
    /** 模糊查询key */
    private String queryKey;
}
