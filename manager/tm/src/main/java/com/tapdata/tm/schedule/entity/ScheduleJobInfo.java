package com.tapdata.tm.schedule.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.schedule.constant.ScheduleJobEnum;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@Builder
@Document("ScheduleJobInfo")
public class ScheduleJobInfo extends BaseEntity {
    private String groupName;
    private String jobName;
    @Schema(description = "定时任务标识")
    private String code;
    private String cron;
    @Schema(description = "定时任务执行类")
    private String className;
    @Schema(description = "成功执行次数")
    private Integer succeed;
    @Schema(description = "失败执行次数")
    private Integer fail;
    @Schema(description = "任务的状态 正在执行 已删除 暂停")
    private ScheduleJobEnum status;
}
