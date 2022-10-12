package com.tapdata.tm.discovery.service;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;

import java.util.List;

/**
 * 默认目录
 */
public interface DefaultDataDirectoryService {
    void addConnection(String connectionId, UserDetail user);
    void removeConnection(String connectionId, UserDetail user);
    void updateConnection(DataSourceConnectionDto connectionDto, UserDetail user);
    void addConnection(DataSourceConnectionDto connectionDto, UserDetail user);
    void addConnections(UserDetail user);

    void deleteDefault(UserDetail user);
    void addConnections(List<DataSourceConnectionDto> connectionDto, UserDetail user);

    void addPdkIds(UserDetail user);

    void addJobs(UserDetail user);

    void addApi(UserDetail user);
}
