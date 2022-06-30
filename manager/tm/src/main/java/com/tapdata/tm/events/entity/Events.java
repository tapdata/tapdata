package com.tapdata.tm.events.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@EqualsAndHashCode(callSuper=false)
@Document("Events")
public class Events extends BaseEntity {
    private String groupId;

    private EventData event_data;

    private FailResult failed_result;

    private String event_status;
    private String job_id;
    private  String name;
    private String receivers;


    private String tag;
    private String type;
    private String sendGroupId;
    private  Long sendGroupTTL;
    private Boolean lock;



}
