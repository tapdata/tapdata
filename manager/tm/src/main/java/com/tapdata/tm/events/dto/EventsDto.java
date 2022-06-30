package com.tapdata.tm.events.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.events.entity.EventData;
import com.tapdata.tm.events.entity.FailResult;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class EventsDto extends BaseDto {
    private String groupId;

    private EventData event_data;

    private FailResult failResult;

    private Boolean event_status;
    private String job_id;
    private  String name;
    private String receivers;


    private String tag;
    private String type;
    private String sendGroupId;
    private  Long sendGroupTTL;
    private Boolean lock;



}
