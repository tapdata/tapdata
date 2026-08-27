package com.tapdata.tm.userLog.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;


@Data
@EqualsAndHashCode(callSuper=false)
public class UserLogDto extends BaseDto {

    public String ip;
    public String biz_module;
    public String desc;
    public String url;
    public User user;
    private String modular;
    private String operation;
    private String parameter1;
    private String parameter2;
    private String parameter3;
    private String modelName;

    private String messageId;
    private String username;
    private String i18nMessage;
    private String type;
    private String eventType;
    private String outcome;
    private String failureReason;
    private String eventId;
    private String objectName;
    private String changeSummary;
    private String serviceNode;
    private String componentType;
    private String instanceName;
    private String loginMethod;
    //是否修改名称
    private Boolean rename;


    private ObjectId sourceId;

}
