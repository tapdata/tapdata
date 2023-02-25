package com.tapdata.tm.hazelcastPersistence.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;


/**
 * Hazelcast Persistence
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class HazelcastPersistenceDto extends BaseDto {
    private String imap;
    private String key;
    private Map<String, Object> value;
}