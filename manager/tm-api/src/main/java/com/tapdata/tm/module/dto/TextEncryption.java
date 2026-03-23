package com.tapdata.tm.module.dto;

import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/19 22:42 Create
 * @description
 */
@Data
public class TextEncryption {
    String id;
    /**
     * Regular expression: specific matching rules
     */
    String regex;

    /**
     * replacement char
     */
    String outputChar;

    /**
     * replacement type
     * see com.tapdata.tm.system.api.enums.OutputType
     */
    Integer outputType;

    /**
     * Replace the quantity by replacing the identified consecutive characters with the replacement symbol ${outputChar}
     */
    Integer outputCount;

    public static void textEncryptionFromId(Param param, Map<String, TextEncryption> textEncryptionMap) {
        List<String> textEncryptionRuleIds = param.getTextEncryptionRuleIds();
        if (CollectionUtils.isEmpty(textEncryptionRuleIds)) {
            return;
        }
        List<TextEncryption> list = textEncryptionRuleIds.stream()
                .filter(StringUtils::isNotBlank)
                .map(textEncryptionMap::get)
                .filter(Objects::nonNull)
                .toList();
        param.setTextEncryptionRule(list);
        param.setTextEncryptionRuleIds(null);
    }
}
