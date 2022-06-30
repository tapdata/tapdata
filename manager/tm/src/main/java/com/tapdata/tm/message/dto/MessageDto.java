package com.tapdata.tm.message.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.message.constant.MessageMetadata;
import com.tapdata.tm.message.constant.SourceModuleEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import lombok.Data;

import javax.validation.constraints.NotNull;
import java.util.Date;



@Data
public class MessageDto {

    private String id;

    private String level;
    private SystemEnum systemEnum;
    private String system;

    private String agentName;
    private String agentId;

    private String msg;
    private String title;

    private String serverName;

    private MessageMetadata messageMetadataObject;
    private SourceModuleEnum sourceModuleEnum;


    //其实就对应实体类的msg
    private String notification;
    private String sourceModule;
    private String messageMetadata;


    private String sourceId;
    private Boolean agentTags;

    private String tcmUserId;

    private String phoneNumber;

    @NotNull
    private String userId;

    private String email;

    private String username;

    private String mappingTemplate;
    private Boolean read = false;
    private Boolean isDeleted = false;

    private String time;
    private String last_updated;

    @JsonProperty("createTime")
    private String createAt;

    private String groupId;

    private String monitorName;

}