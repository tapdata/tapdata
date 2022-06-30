package com.tapdata.tm.datarules.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * DataRules
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("DataRules")
public class DataRulesEntity extends BaseEntity {
    private String name;

}