package com.tapdata.tm.lock.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * mongodb 分布式锁
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "Mongo_lock_doc")
public class LockDocument {
    @Id
    private String id;
    private long expireAt;
    private String token;
}