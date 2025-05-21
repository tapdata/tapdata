package com.tapdata.tm.taskinspect.cons;

/**
 * 全量自定义模式-校验触发方式
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 15:25 Create
 */
public enum CustomFullTriggerEnum {
    AUTO, // 全量完成后触发
    CRON, // 定时调度
    ;
}
