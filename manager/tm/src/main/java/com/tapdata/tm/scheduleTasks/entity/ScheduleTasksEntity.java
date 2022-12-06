package com.tapdata.tm.scheduleTasks.entity;

import com.mongodb.BasicDBObject;
import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/10 下午3:30
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ScheduleTasks")
public class ScheduleTasksEntity extends BaseEntity {

    private String task_type;
    private String type;
    private Long period;
    private String status;
    private String task_name;
    private String task_profile;
    private String agent_id;
    private Long ping_time;
    private ArrayList<BasicDBObject> thread;
    private ArrayList<Object> filter;
    private Map<String, Object> task_data;
    private Object statsOffset;

}
