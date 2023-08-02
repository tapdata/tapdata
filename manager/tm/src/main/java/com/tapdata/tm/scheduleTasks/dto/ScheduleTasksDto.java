package com.tapdata.tm.scheduleTasks.dto;

import com.mongodb.BasicDBObject;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.worker.dto.WorkSchedule;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper=false)
public class ScheduleTasksDto extends BaseDto {

    private String task_type;
    private String type;
    private Long period;
    private String status;
    private String task_name;
    private String task_profile;
    private String agent_id;
    private Date last_updated;
    private Long ping_time;
    private ArrayList<WorkSchedule> thread;
    private Map<String, Object> task_data;
    private String filter;
    private Object statsOffset;

}
