package com.tapdata.tm.taskinspect.cons;

/**
 * 表过滤配置类型
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/12/2 10:23 Create
 */
public enum TableFilterTypeEnum {
    NONE,          // 关闭
    INCLUDES,      // 包含表
    INCLUDE_REGEX, // 表达式匹配表
    EXCLUDES,      // 排除表
    EXCLUDE_REGEX, // 表达式匹配表
}
