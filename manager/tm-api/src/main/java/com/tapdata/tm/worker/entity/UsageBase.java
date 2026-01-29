package com.tapdata.tm.worker.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

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
     * 0: 5S颗粒度点位
     * 1: 1分钟颗粒度点位
     * 2: 1小时颗粒度点位
     * null: 零散点位
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
     * 只有当processType=3时，workOid=worker id
     */
    protected String workOid;

    protected Double cpuUsage;

    protected Double selfCpuUsage;

    protected Long heapMemoryMax;

    protected Long heapMemoryUsage;

    protected Long lastUpdateTime;

    protected Date ttlKey;
}
