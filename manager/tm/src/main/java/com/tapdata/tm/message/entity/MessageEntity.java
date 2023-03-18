package com.tapdata.tm.message.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.message.constant.MessageMetadata;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.Map;

/**
 * message表的userId是不带下划线的，所以需要做特别兼容
 */
@Document("Message")
@Data
@EqualsAndHashCode(callSuper=false)
public class MessageEntity extends BaseEntity {
    private String level;
    private String system;
    private String msg;
    private String title;

    private String agentId;
//    private String agentName;

    @Field("agentName")
    private String serverName;

    private String sourceId;

    private MessageMetadata messageMetadata;
    private String messageEventType;
    private String sourceModule;

    private String email;

    private String username;

    private String mappingTemplate;
    private Boolean read;
    private Date time;
    private String groupId;
    private Boolean isDeleted;

    /**
     * 兼容旧数据
     */
    @Field("userId")
    private String oldUserId;

    private String monitorName;

    private String template;
    private Map<String, Object> param;
}
