package com.tapdata.tm.worker.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/12 15:25 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("MetricInfo")
public class MetricInfoEntity extends BaseEntity {
    /**
     * unit byte
     * */
    Long heapMemoryUsage;

    /**
     * unit %, such as 0.1 means 10%
     * */
    Double cpuUsage;

    /**
     * timestamp, unit ms
     * */
    Long lastUpdateTime;

    /**
     * api-server
     * worker
     * task
     * */
    String type;

    /**
     * type === api-server, processId === api-server id
     * type === worker, processId === worker id
     * type === task, processId === task id
     * */
    String nodeId;

    boolean delete;
}
