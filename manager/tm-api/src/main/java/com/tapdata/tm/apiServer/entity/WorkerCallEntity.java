package com.tapdata.tm.apiServer.entity;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/2 15:10 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ApiCallInWorker")
public class WorkerCallEntity extends BaseEntity {

    /**
     * ApiServer ID
     * */
    private String processId;

    /**
     * Worker process ID
     * */
    private String workOid;

    private String apiId;

    /**
     * The number of requests at the current time granularity
     * */
    private Long reqCount;

    /**
     * RPS
     * */
    private Double rps;

    /**
     * Delay list at current time granularity, in milliseconds
     * */
    private List<Map<String, Number>> delays;
    /**
     * P50=median at current time granularity, 50% of request delays are below this value, in milliseconds
     * */
    private Long p50;
    /**
     * At the current time granularity, P95=95% of requests have a latency lower than this value, measured in milliseconds
     * */
    private Long p95;
    /**
     * At the current time granularity, P99=99% of requests have a latency lower than this value, measured in milliseconds
     * */
    private Long p99;

    /**
     * The number of failed requests at the current time granularity
     * */
    private Long errorCount;
    /**
     * Exception rate at current time granularity: number of failures/number of requests
     * */
    private Double errorRate;

    /**
     * The statistical start time at the current time granularity, with millisecond level timestamps (accurate to the minute level)
     * */
    private Long timeStart;

    /**
     * time granularity
     * 1 - minute
     * 2 - hour
     * 3 - day
     *
     * @see TimeGranularity
     * */
    private int timeGranularity;

    private Date ttlKey;

    private Boolean delete;

    private ObjectId lastApiCallId;
}
