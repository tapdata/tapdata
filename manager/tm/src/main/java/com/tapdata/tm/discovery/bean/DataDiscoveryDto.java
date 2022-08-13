package com.tapdata.tm.discovery.bean;

import com.tapdata.tm.commons.schema.Tag;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * 数据发现概览
 */
@Data
public class DataDiscoveryDto {

    private String id;
    /** 对象分类 */
    private DataObjCategoryEnum category;
    /** 对象类型 */
    private String type;
    /** 来源分类 */
    private DataSourceCategoryEnum sourceCategory;
    /** 来源类型 */
    private String sourceType;
    /** 来源信息 */
    private String sourceInfo;
    /** 对象名称 */
    private String name;
    /** 业务名称 */
    private String businessName;
    /** 业务描述 */
    private String businessDesc;
    /** 目录标签 */
    private List<Tag> listtags;
    private List<Tag> allTags;

}
