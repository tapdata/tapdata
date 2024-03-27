package com.tapdata.tm.base.dto.ds;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/17 下午3:58
 * @description
 */
@Data
public class UpdateDto<T extends DsBaseDto> {

    @JsonProperty("$set")
    private T set;

    @JsonProperty("$setOnInsert")
    private T setOnInsert;

    @JsonProperty("$unset")
    private Map<String, Object> unset;
}
