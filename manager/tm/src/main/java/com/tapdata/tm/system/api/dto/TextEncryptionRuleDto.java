package com.tapdata.tm.system.api.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/11 14:39 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TextEncryptionRuleDto extends BaseDto {
    /**
     * 规则名称
     * */
    String name;

    /**
     * 规则描述
     * */
    String description;

    /**
     * 正则表达式：具体的匹配规则
     * */
    String regex;

    /**
     * 替换符号
     * */
    String outputChar;

    /**
     * 替换类型
     * @see com.tapdata.tm.system.api.enums.OutputType
     * */
    Integer outputType;

    /**
     * 替换数量，将识别出的连续字符用替换符号替换为${outputChar}的个数
     * */
    Integer outputCount;
}
