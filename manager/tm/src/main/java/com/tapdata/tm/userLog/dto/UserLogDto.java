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
    private String type;
    //是否修改名称
    private Boolean rename;


    private ObjectId sourceId;

}
