package com.tapdata.tm.taskinspect.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.TaskInspectMode;
import com.tapdata.tm.taskinspect.config.Custom;
import com.tapdata.tm.taskinspect.config.Intelligent;
import com.tapdata.tm.taskinspect.config.TableFilter;
import com.tapdata.tm.taskinspect.cons.TimeCheckModeEnum;
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
    private Boolean enable;                  // 是否开启校验
    private TaskInspectMode mode;            // 校验模式
    private Intelligent intelligent;         // 智能校验配置
    private Custom custom;                   // 自定义校验配置
    private Integer queueCapacity;           // 队列最大容量
    private Long cdcTimeout;                 // 增量校验在队列中超时，不作延迟等待
    private Boolean checkNoPkTable;          // 校验无主键表
    private TimeCheckModeEnum timeCheckMode; // 时间校验模式
    private TableFilter tableFilter;         // 表过滤

    public TaskInspectDto fill(TaskInspectConfig config) {
        setEnable(config.getEnable());
        setMode(config.getMode());
        setIntelligent(config.getIntelligent());
        setCustom(config.getCustom());
        setQueueCapacity(config.getQueueCapacity());
        setCdcTimeout(config.getCdcTimeout());
        setCheckNoPkTable(config.getCheckNoPkTable());
        setTimeCheckMode(config.getTimeCheckMode());
        return this;
    }
}
