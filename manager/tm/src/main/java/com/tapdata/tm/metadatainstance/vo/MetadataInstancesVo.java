package com.tapdata.tm.metadatainstance.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.vo.BaseVo;
import lombok.*;

import java.util.List;


/**
 * 数据源模型,返回给创建校验任务  页面校验列表使用
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class MetadataInstancesVo extends BaseVo {

    @JsonProperty("meta_type")
    private String metaType;

    @JsonProperty("original_name")
    private String originalName;

    private String databaseId;

    private List<Field> fields;

    private SourceVo source;

    private String database;


}
