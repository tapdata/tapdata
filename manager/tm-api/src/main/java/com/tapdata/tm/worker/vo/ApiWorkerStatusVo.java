package com.tapdata.tm.worker.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class ApiWorkerStatusVo {
    Long serverDate;

    @JsonProperty("worker_status")
    Object workerStatus;
}
