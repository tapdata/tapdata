package com.tapdata.tm.userLog.entity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.userLog.dto.User;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("UserLogs") // 对应数据库表
public class UserLogs extends BaseEntity {
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


    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId sourceId;

}
