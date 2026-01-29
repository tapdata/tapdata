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
     * 工作进程ID
     * */
    private String workOid;

    private String apiId;

    /**
     * 当前时间粒度下的请求数
     * */
    private Long reqCount;

    /**
     * RPS
     * */
    private Double rps;

    /**
     * 当前时间粒度下的延迟列表, 单位ms
     * */
    private List<Map<String, Number>> delays;
    /**
     * 当前时间粒度下P50 = 中位数，50%的请求延迟低于这个值, 单位ms
     * */
    private Long p50;
    /**
     * 当前时间粒度下P95 = 95%的请求延迟低于这个值, 单位ms
     * */
    private Long p95;
    /**
     * 当前时间粒度下P99 = 99%的请求延迟低于这个值, 单位ms
     * */
    private Long p99;

    /**
     * 当前时间粒度下的请求失败数
     * */
    private Long errorCount;
    /**
     * 当前时间粒度下的异常率：失败数/请求数
     * */
    private Double errorRate;

    /**
     * 当前时间粒度的统计开始时间,毫秒级时间戳（精确到分钟级别）
     * */
    private Long timeStart;

    /**
     * 时间粒度
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
