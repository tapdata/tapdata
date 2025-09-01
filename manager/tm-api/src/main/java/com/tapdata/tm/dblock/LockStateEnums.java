package com.tapdata.tm.dblock;

import lombok.Getter;

/**
 * 数据库锁-更新结果状态
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/8 14:50 Create
 */
@Getter
public enum LockStateEnums {
    YES(true),        // 加锁成功
    YES_CHANGE(true), // 加锁成功，变更拥有者
    NO(false),        // 加锁失败
    ;

    private final boolean isYes;

    LockStateEnums(boolean isYes) {
        this.isYes = isYes;
    }

}
