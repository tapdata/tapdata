package com.tapdata.tm.modules.vo;

import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ApiDefinitionVo {
    List apis;
    List<ConnectionVo> connections;

    /**
     * api所使用到的文本加密规则配置
     * */
    Map<String, TextEncryptionRuleDto> textEncryptionRules;
}
