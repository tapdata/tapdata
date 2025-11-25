package com.tapdata.tm.task.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.task.param.TableLogCollectorParam;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @Author: Zed
 * @Date: 2022/2/15
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class LogCollectorServiceImpl implements LogCollectorService {
    @Override
    public Page<LogCollectorVo> find(Filter filter, UserDetail user) {
        Page<LogCollectorVo> logCollectorVoPage = new Page<>();
        return logCollectorVoPage;
    }

    @Override
    public List<TaskDto> findSyncTaskById(TaskDto taskDto, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<LogCollectorVo> findByTaskId(String taskId, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<LogCollectorVo> findBySubTaskId(String taskId, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<LogCollectorVo> findByConnectionName(String name, String connectionName, UserDetail user, int skip, int limit, List<String> sort) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public boolean checkCondition(UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void update(LogCollectorEditVo logCollectorEditVo, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public LogCollectorDetailVo findDetail(String id, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public LogSystemConfigDto findSystemConfig(UserDetail loginUser) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void updateSystemConfig(LogSystemConfigDto logSystemConfigDto, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Boolean checkUpdateConfig(UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Boolean checkUpdateConfig(String connectionId, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<Map<String, String>> findTableNames(String taskId, int skip, int limit, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<Map<String, String>> findCallTableNames(String taskId, String callSubId, int skip, int limit, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void logCollector(UserDetail user, TaskDto oldTaskDto) {
    }

    @Override
    public void startConnHeartbeat(UserDetail user, TaskDto taskDto) {
    }

    @Override
    public void endConnHeartbeat(UserDetail user, TaskDto taskDto) {
    }

    @Override
    public void cancelMerge(String taskId, String connectionId, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<ShareCdcTableInfo> tableInfos(String taskId, String connectionId, String keyword, Integer page, Integer size, String order, UserDetail user) {
        Page<ShareCdcTableInfo> shareCdcTableInfoPage = new Page<>();
        return shareCdcTableInfoPage;
    }

    @Override
    public Page<ShareCdcTableInfo> excludeTableInfos(String taskId, String connectionId, String keyword, Integer page, Integer size, String order, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void configTables(String taskId, List<TableLogCollectorParam> params, String type, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<ShareCdcConnectionInfo> getConnectionIds(String taskId, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void clear() {
    }

    @Override
    public void removeTask() {
    }
}
