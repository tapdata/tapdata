package com.tapdata.tm.commons.base.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/17 下午3:58
 * @description
 */
@Data
public class UpdateDto<T extends BaseDto> implements Serializable {

    @JsonProperty("$set")
    private T set;

    @JsonProperty("$setOnInsert")
    private T setOnInsert;

    @JsonProperty("$unset")
    private Map<String, Object> unset;

}
