package com.tapdata.tm.userLog.dto;


import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * 对应userLong  里的user 属性
 * user_id : "61406996c4e5c40012662555"
 * email: ""
 * username :"18661673206"
 */
@Data
public class User {
    @Field("user_id")
    private String userId;
    private String email;
    private String username;
}
