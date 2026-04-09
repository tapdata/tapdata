package com.tapdata.tm.module.dto;

import com.tapdata.tm.module.enums.ParamTypeEnum;
import lombok.Data;

import java.util.List;

@Data
public class Param {
    private String name;

    /**
     * @see ParamTypeEnum
     * */
    private String type;

    private String defaultvalue;

    private String description;

    private boolean required;

    /**
     * Sensitive Fields - Encryption Rule List
     * */
    private List<String> textEncryptionRuleIds;

    /**
     * Sensitive Fields - Encryption Rule List
     * Used by apiserver for field encryption, see: apiserver/monitor.js 103
     * */
    private List<TextEncryption> textEncryptionRule;
}
