package com.tapdata.tm.commons.schema.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2021/9/2
 * @Description: connect内嵌的url info文档
 */
@AllArgsConstructor
@Getter
@Setter
public class UrlInfo {

    /**
     * 路径
     */
    private String url;
    /**
     * 是否递归
     */
    private boolean method;
    @JsonProperty("urlType")
    @Field("urlType")
    private String url_type;
    @JsonProperty("contentType")
    @Field("contentType")
    private String content_type;
    private Map<String, Object> headers;
    @JsonProperty("requestParameters")
    @Field("requestParameters")
    private Map<String, Object> requestParameters;
    @JsonProperty("offset_field")
    @Field("offset_field")
    private String offsetField;
    @JsonProperty("initial_offset")
    @Field("initial_offset")
    private String initialOffset;

}
