package com.tapdata.tm.modules.dto;

import lombok.Data;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/12 11:29 Create
 * @description
 */
@Data
public class ParamEncryptionRule {
    String paramName;
    List<String> ruleId;
}
