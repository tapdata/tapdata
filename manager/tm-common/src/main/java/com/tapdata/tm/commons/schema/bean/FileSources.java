package com.tapdata.tm.commons.schema.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * @Author: Zed
 * @Date: 2021/9/2
 * @Description: connect 内嵌的file source文档
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class FileSources {

    /**
     * 路径
     */
    private String path;
    /**
     * 是否递归
     */
    private boolean recursive;
    private String selectFileType;
    @JsonProperty("include_filename")
    @Field("include_filename")
    private String includeFilename;
    @JsonProperty("exclude_filename")
    @Field("exclude_filename")
    private String excludeFilename;

}
