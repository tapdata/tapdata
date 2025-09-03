package com.tapdata.tm.dblock.repository;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;
import java.util.Date;

/**
 * 数据库锁-持久化实体
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/7 14:36 Create
 */
@Setter
@Getter
@Document("DBLock")
public class MongoDBLockEntity implements Serializable {
    public static final String FIELD_LOCK_KEY = "_id";
    public static final String FIELD_OWNER = "owner";
    public static final String FIELD_CREATED = "created";
    public static final String FIELD_UPDATED = "updated";
    public static final String FIELD_EXPIRED = "expired";

    @Id
    @Field("_id")
    private String lockKey; // 锁名称
    private String owner;   // 获取者标识（例如 hostname:pid:thread）
    private Date created;   // 创建时间
    private Date updated;   // 更新时间
    private Date expired;   // 到期时间
}
