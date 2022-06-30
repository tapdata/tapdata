//package com.tapdata.tm.userLog.vo;
//
//import com.alibaba.fastjson.annotation.JSONField;
//import com.fasterxml.jackson.annotation.JsonFormat;
//import com.fasterxml.jackson.annotation.JsonProperty;
//import com.google.gson.annotations.SerializedName;
//import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
//import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
//import com.tapdata.tm.userLog.dto.User;
//import lombok.Data;
//import org.bson.types.ObjectId;
//import org.springframework.data.mongodb.core.index.Indexed;
//
//import java.util.Date;
//
//@Data
//public class UserLogVo {
//
///*    @JSONField(serializeUsing = ObjectIdSerialize.class, deserializeUsing = ObjectIdDeserialize.class)
//    不起作用，待考证*/
//    private String id;
//
//    private String modular;
//    private String operation;
//    private String parameter1;
//    private String parameter2;
//    private String parameter3;
//    private String username;
//
//    private Date createAt;
//
//    private String type;
//
//    @JsonProperty("last_updated")
//    private Date lastUpdAt;
//
//
//
//}
