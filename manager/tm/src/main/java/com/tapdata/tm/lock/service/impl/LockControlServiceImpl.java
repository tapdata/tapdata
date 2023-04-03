package com.tapdata.tm.lock.service.impl;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.lock.annotation.Lock;
import com.tapdata.tm.lock.constant.LockType;
import com.tapdata.tm.lock.service.LockControlService;
import org.springframework.stereotype.Service;


@Service
public class LockControlServiceImpl implements LockControlService {

    /**
     * 挖掘任务启动需要一个队列，防止混乱
     */
    @Override
    @Lock(value = "user.userId", type = LockType.START_LOG_COLLECTOR, expireSeconds = 15)
    public void logCollectorStartQueue(UserDetail user) {
    }

    /**
     * FDM任务启动需要一个队列，防止混乱
     */
    @Override
    @Lock(value = "user.userId", type = LockType.START_LDP_FDM, expireSeconds = 15)
    public void fdmStartQueue(UserDetail user) {
    }
}