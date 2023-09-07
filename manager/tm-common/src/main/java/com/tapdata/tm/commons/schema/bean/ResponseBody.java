
package com.tapdata.tm.commons.schema.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;
import java.util.List;

@Data
public class ResponseBody implements Serializable {

    private static final long serialVersionUID = 6282824794422216838L;
    /** 重试次数 */
    private Long retry;
    /**  校验详情 */
    @JsonProperty("validate_details")
    @Field("validate_details")
    private List<ValidateDetail> validateDetails;

}
