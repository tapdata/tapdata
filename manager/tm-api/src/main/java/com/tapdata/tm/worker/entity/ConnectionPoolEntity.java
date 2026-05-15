package com.tapdata.tm.worker.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/4/29 14:29 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ConnectionPool")
public class ConnectionPoolEntity extends BaseEntity {
    String connectionId;

    /**
     * @see com.tapdata.tm.apiServer.enums.TimeGranularity
     * */
    int timeGranularity;

    /**
     * api server id
     */
    protected String processId;

    Long lastUpdateTime;

    String dataType;

    Integer maxConnections;

    Integer usedConnections;

    Integer available;

    Integer queueSize;

    protected Date ttlKey;
}
