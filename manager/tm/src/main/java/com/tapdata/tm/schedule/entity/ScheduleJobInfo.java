package com.tapdata.tm.schedule.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Getter
@Setter
@AllArgsConstructor
@Builder
@Document("ScheduleJobInfo")
public class ScheduleJobInfo extends BaseEntity {
    private String groupName;
    private String jobName;
    private String code;
    private String cron;
    private String className;
    private Integer succeed;
    private Integer fail;
    private String status;
}
