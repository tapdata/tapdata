package com.tapdata.tm.commons.base.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;


@Data
@EqualsAndHashCode(callSuper = true)
public class SchedulableDto extends  BaseDto {

    private String agentId; //调度到指定的实例上去
    private String hostName;
    private List<String> agentTags; // 标签

    private Integer scheduleTimes;  // 调度次数
    private Long scheduleTime;  // 上次调度时间

}
