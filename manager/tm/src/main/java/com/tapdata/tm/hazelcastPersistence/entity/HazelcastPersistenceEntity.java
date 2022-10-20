package com.tapdata.tm.hazelcastPersistence.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;


/**
 * Hazelcast Persistence
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("HazelcastPersistence")
public class HazelcastPersistenceEntity extends BaseEntity {
    private String imap;
    private String key;
    private Map<String, Object> value;
}