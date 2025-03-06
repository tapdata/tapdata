package com.tapdata.huawei.drs.kafka;

/**
 * 阶段
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/18 15:57 Create
 */
public enum Stage {
    CDC,  // 增量
    INIT, // 全量
    ;
}
