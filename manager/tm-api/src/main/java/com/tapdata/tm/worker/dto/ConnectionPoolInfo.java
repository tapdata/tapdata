package com.tapdata.tm.worker.dto;

import com.tapdata.tm.worker.entity.ConnectionPoolEntity;
import lombok.Data;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/4/21 09:24 Create
 * @description
 */
@Data
public class ConnectionPoolInfo {

    int poolMaxConnections;

    int poolUsedConnections;

    int poolAvailable;

    int poolQueueSize;

    long lastUpdateTime;

    List<ConnectionPoolEntity> connections;
}
