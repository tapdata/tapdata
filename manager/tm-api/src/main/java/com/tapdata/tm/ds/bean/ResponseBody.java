
package com.tapdata.tm.ds.bean;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.bean.ValidateDetail;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

@Data
public class ResponseBody {

    /** 重试次数 */
    private Long retry;
    /**  校验详情 */
    @JsonProperty("validate_details")
    @Field("validate_details")
    private List<ValidateDetail> validateDetails;

}
