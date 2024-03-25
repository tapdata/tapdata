package com.tapdata.tm.accessToken.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;

@Data
@Document("AccessToken")
@AllArgsConstructor
@NoArgsConstructor
public class AccessTokenEntity {

    @Field("_id")
    private String id;
    private Long ttl;
    private Date created;
    private ObjectId userId;

    @Field("last_updated")
    private Date lastUpdated;

    private  String authType;

}
