package com.tapdata.tm.lock.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.lock.annotation.Lock;
import com.tapdata.tm.lock.constant.LockType;

public interface LockControlService {

    /**
     * 挖掘任务启动需要一个队列，防止混乱
     */
    void logCollectorStartQueue(UserDetail user);

    /**
     * FDM任务启动需要一个队列，防止混乱
     */
    void fdmStartQueue(UserDetail user);
}
