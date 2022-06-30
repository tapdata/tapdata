package com.tapdata.tm.metaData.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class MetaDataVo extends BaseVo {

    private String meta_type;

    @JsonProperty("original_name")
    private String originalName;


    private SourceDto source;

    private String database;
}
