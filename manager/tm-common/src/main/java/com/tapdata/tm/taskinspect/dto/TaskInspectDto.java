package com.tapdata.tm.taskinspect.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.taskinspect.TaskInspectMode;
import com.tapdata.tm.taskinspect.config.Custom;
import com.tapdata.tm.taskinspect.config.Intelligent;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 任务校验数据映射
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/9 21:58 Create
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class TaskInspectDto extends BaseDto {
    private Boolean enable; // 是否开启校验
    private TaskInspectMode mode; // 校验模式
    private Intelligent intelligent; // 智能校验配置
    private Custom custom; // 自定义校验配置
}
