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
     * 规则名称
     * */
    @Field("name")
    String name;

    /**
     * 规则类型
     * @see com.tapdata.tm.system.api.enums.RuleType
     * */
    @Field("type")
    Integer type;

    /**
     * 规则描述
     * */
    @Field("description")
    String description;

    /**
     * 正则表达式：具体的匹配规则
     * */
    @Field("regex")
    String regex;

    /**
     * 替换符号
     * */
    @Field("outputChar")
    String outputChar;

    /**
     * 替换类型
     * @see com.tapdata.tm.system.api.enums.OutputType
     * */
    @Field("outputType")
    Integer outputType;

    /**
     * 替换数量，将识别出的连续字符用替换符号替换为${outputChar}的个数
     * */
    @Field("outputCount")
    Integer outputCount;

    /**
     * 删除标记，1表示被删除
     * */
    @Field("deleted")
    Integer deleted;
}
