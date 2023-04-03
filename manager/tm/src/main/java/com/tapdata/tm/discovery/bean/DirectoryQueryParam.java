package com.tapdata.tm.discovery.bean;

import lombok.Data;

@Data
public class DirectoryQueryParam {
    /** directoryId */
    private String id;
    /** 来源类型 */
    private String objType;
    /**  */
    private Integer page = 1;
    /**  */
    private Integer pageSize = 20;
    /** 模糊查询key */
    private String queryKey;
    /** 分类id */
    private String tagId;

    /** 查询的范围 current 当前分类下，  默认：currentAndChild 当前及其子类，包含子类的子类*/
    private String range = "currentAndChild";

    /** true 或者为空的时候，会查询关联查询表名，表描述，表别名等，false只会模糊查询表名 */
    private Boolean regUnion;
}
