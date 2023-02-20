package com.tapdata.tm.inspect.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.inspect.bean.Limit;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.bean.Timing;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;

import java.util.List;

/**
 * 提供返回给页面的vo类
 * 暂时没有用到
 */
@Data
public class InspectListVo extends BaseVo {
    /**  */
    private String name;
    /**  */
    private String flowId;
    /**  */
    private String mode;
    /**  */
    private String inspectMethod;

    /**  */
    private Boolean enabled=true;
    /**  */
//    private List<Task> tasks;
    /**  */
    private String dataFlowName;
    /**  */
    private String status;
    /**  */
    private Long lastStartTime;
    /**  */
    private String errorMsg;

    /**
     * 调用该方法，result  总是返回空字符串，导致inspect表更新总是不正确，因此需要另作调整
     */
    private String result;

    @JsonProperty("difference_number")
    private Integer differenceNumber=0;

    @JsonProperty("InspectResult")
    private InspectResultListVo inspectResult;

    /**
     * 标志是否是二次校验，如果是，值就是父校验的id,
     * 如果不是，就 空
     */
    private String byFirstCheckId;
    private String version; // 校验任务编辑时生成版本号，用于表示结果属于哪个版本
}
