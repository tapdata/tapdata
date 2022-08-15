package com.tapdata.tm.verison.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * Version of the tapdata
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("version")
public class VersionEntity extends BaseEntity {
    private String key;
    private String value;

}