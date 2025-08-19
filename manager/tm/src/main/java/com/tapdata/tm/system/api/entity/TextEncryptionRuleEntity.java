package com.tapdata.tm.system.api.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/11 14:40 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("TextEncryptionRule")
public class TextEncryptionRuleEntity extends BaseEntity {
    /**
     * Rule name
     * */
    @Field("name")
    String name;

    /**
     * Rule type
     * @see com.tapdata.tm.system.api.enums.RuleType
     * */
    @Field("type")
    Integer type;

    /**
     * rule description
     * */
    @Field("description")
    String description;

    /**
     * regex: Specific matching rules
     * */
    @Field("regex")
    String regex;

    /**
     * Replace symbol
     * */
    @Field("outputChar")
    String outputChar;

    /**
     * Replace type
     * @see com.tapdata.tm.system.api.enums.OutputType
     * */
    @Field("outputType")
    Integer outputType;

    /**
     * Replace the quantity by using replacement symbols to replace the recognized consecutive characters with the number of ${outputted Char}
     * */
    @Field("outputCount")
    Integer outputCount;

    /**
     * Delete marker, 1 indicates deleted
     * */
    @Field("deleted")
    Integer deleted;

    /**
     * Name: Multilingual Code
     * */
    @Field("nameLangCode")
    String nameLangCode;

    /**
     * Description Text Multilingual Code
     * */
    @Field("descriptionLangCode")
    String descriptionLangCode;

    @Field("sort")
    int sort;
}
