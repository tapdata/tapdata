package com.tapdata.tm.dictionary.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * Dictionary
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Dictionary")
public class DictionaryEntity extends BaseEntity {
    private String name;

}