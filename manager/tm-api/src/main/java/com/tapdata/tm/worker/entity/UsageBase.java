package com.tapdata.tm.worker.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.worker.dto.ConnectionPoolInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/21 23:20 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class UsageBase extends BaseEntity {
    /**
     * 0: 5S particle size point location
     * 1: 1-minute granularity point location
     * 2: 1-hour particle size point location
     * null: Scattered locations
     */
    protected Integer type;
    /**
     * 0:tm
     * 1:engine
     * 2:api server
     * 3:api server worker
     * @see ServerUsage.ProcessType
     */
    protected int processType;
    /**
     * processType=0, processId=tm id
     * processType=1, processId=engine id
     * processType=2, processId=api server id
     * processType=3, processId=api server id
     */
    protected String processId;

    /**
     * Only when processType=3, workOid=worker id
     */
    protected String workOid;

    protected Double cpuUsage;

    protected Double selfCpuUsage;

    protected Long heapMemoryMax;

    protected Long heapMemoryUsage;

    protected Long lastUpdateTime;

    protected Integer poolMaxConnections;

    protected Integer poolUsedConnections;

    protected Integer poolAvailable;

    protected Integer poolQueueSize;

    protected List<ConnectionPoolEntity> poolConnections;

    protected Integer cpuCores;

    protected Date ttlKey;

    protected Integer currentCpuUsage;

    public void withConnectionPollInfo(ConnectionPoolInfo info) {
        if (null == info) {
            return;
        }
        this.poolAvailable = info.getPoolAvailable();
        this.poolMaxConnections = info.getPoolMaxConnections();
        this.poolUsedConnections = info.getPoolUsedConnections();
        this.poolQueueSize = info.getPoolQueueSize();
        this.poolConnections = info.getConnections();
    }
}
