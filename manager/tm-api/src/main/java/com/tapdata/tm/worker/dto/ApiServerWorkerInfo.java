package com.tapdata.tm.worker.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/17 14:44 Create
 * @description
 */
@Data
public class ApiServerWorkerInfo {
    Long activeTime;

    Integer id;

    Integer pid;

    String oid;

    String name;

    @Field("worker_status")
    @JsonProperty("worker_status")
    String workerStatus;

    @Field("worker_start_time")
    @JsonProperty("worker_start_time")
    Long workerStartTime;

    MetricInfo metricValues;

    int sort;
}
