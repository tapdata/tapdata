package com.tapdata.tm.inspect.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.inspect.bean.Limit;
import com.tapdata.tm.inspect.bean.Timing;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;

/**
 * 提供返回给页面的vo类
 */
@Deprecated
@Data
public class InspectVo extends BaseVo {
    /**  */
    private String name;
    /**  */
    private String flowId;
    /**  */
    private String mode;
    /**  */
    private String inspectMethod;

    /**  */
    private PlatformInfo platformInfo;
    /**  */
    private Timing timing;
    /**  */
    private Limit limit;
    /**  */
    private Boolean enabled;
    private String status;

    /**  */
    private Long lastStartTime;
    /**  */
    private String errorMsg;
    /**  */
    private String result;

    @JsonProperty("difference_number")
    private Integer differenceNumber=0;

    @JsonProperty("InspectResult")
    private InspectResultDto inspectResult;

}


