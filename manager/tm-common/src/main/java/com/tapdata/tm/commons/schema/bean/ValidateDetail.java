
package com.tapdata.tm.commons.schema.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;

@Data
public class ValidateDetail implements Serializable {

    private static final long serialVersionUID = -7975659308570408391L;
    /** 测试项目代码  */
    @JsonProperty("stage_code")
    @Field("stage_code")
    private String stageCode;
    /**  测试项目名称 */
    @JsonProperty("show_msg")
    @Field("show_msg")
    private String showMsg;
    private String status;
    private Integer sort;
    /** 测试失败消息code */
    @JsonProperty("error_code")
    @Field("error_code")
    private String errorCode;
    /** 是否必须 */
    private Boolean required;
    @JsonProperty("fail_message")
    @Field("fail_message")
    private String failMessage;


}
