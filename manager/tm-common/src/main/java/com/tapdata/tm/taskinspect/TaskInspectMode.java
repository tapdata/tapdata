package com.tapdata.tm.taskinspect;

import lombok.Getter;

/**
 * 校验模式
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 15:24 Create
 */
@Getter
public enum TaskInspectMode {
    CLOSE("-"), // 关闭校验
    INTELLIGENT("-"), // 智能校验
    CUSTOM("com.tapdata.taskinspect.mode.CustomMode"), // 高级校验
    ;

    private final String implClassName;

    TaskInspectMode(String implClassName) {
        this.implClassName = implClassName;
    }
}
