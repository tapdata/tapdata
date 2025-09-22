package com.tapdata.tm.worker.dto;

import lombok.Data;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/1 10:02 Create
 * @description
 */
@Data
public class ApiWorkerInfo {
    Integer id;
    Integer pid;
    String oid;
    String name;
    String description;
    String workerStatus;
    Long workerStartTime;
    Map<String, Object> metricValues;
    Integer sort;

    Long createdTime;
    Long updatedTime;
}
