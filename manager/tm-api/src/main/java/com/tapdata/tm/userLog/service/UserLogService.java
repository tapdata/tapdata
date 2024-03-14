package com.tapdata.tm.userLog.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.dto.UserLogDto;

public interface UserLogService {
    void addUserLog(Modular modular, Operation OperationType, UserDetail userDetail, String sourceId, String parameter1, String parameter2, Boolean rename);

    void addUserLog(Modular modular, Operation OperationType, UserDetail userDetail, String sourceId, String parameter1);

    void addUserLog(Modular modular, Operation OperationType, UserDetail userDetail, String sourceId, String parameter1, Object systemStart);

    void addUserLog(Modular modular, Operation OperationType, UserDetail userDetail, String parameter1);

    void addUserLog(Modular modular, Operation OperationType, String userId, String sourceId, String parameter1);

    void addUserLog(Modular modular, Operation OperationType, String parameter1, UserDetail userDetail);

    Page<UserLogDto> find(Filter filter, UserDetail userDetail);
}
