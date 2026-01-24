package com.tapdata.tm.worker.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/17 14:42 Create
 * @description
 */
@Data
public class ApiServerStatus {

    @JsonProperty("worker_process_id")
    @Field("worker_process_id")
    Integer workerProcessId;

    @JsonProperty("worker_process_start_time")
    @Field("worker_process_start_time")
    Long workerProcessStartTime;

    @JsonProperty("worker_process_end_time")
    @Field("worker_process_end_time")
    Long workerProcessEndTime;

    @JsonProperty("exit_code")
    @Field("exit_code")
    Integer exitCode;



    Integer pid;

    Long activeTime;

    String status;

    Map<String, ApiServerWorkerInfo> workers;

    MetricInfo metricValues;

    private Boolean updateCpuMem;

    Long auditLogPushMaxDelay;
}
