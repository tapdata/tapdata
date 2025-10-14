package com.tapdata.tm.worker.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/10 下午3:57
 * @description
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class WorkerDto extends BaseDto {
    @JsonProperty("process_id")
    @Field("process_id")
    private String processId;
    @JsonProperty("worker_type")
    private String workerType;
    private PlatformInfo platformInfo;
    private Integer weight;
    private List<String> agentTags;
    @JsonProperty("tcm")
    private TcmInfo tcmInfo;
//    @JsonProperty("last_updated")
//    private Date lastUpdated;
    //private long createTime;
    @JsonProperty("ping_time")
    private Long pingTime;
    @JsonProperty("worker_ip")
    private String workerIp;
    private Integer cpuLoad;
    private String hostname;
    @JsonProperty("job_ids")
    private List<String> runningJobs;
    @JsonProperty("running_thread")
    private Integer runningThread;
    @JsonProperty("total_thread")
    private Integer totalThread;
    private Long usedMemory;
    private String version;
    @JsonProperty("accesscode")
    private String accessCode;

    @JsonProperty("userId")
    private String externalUserId; // authing or eCloud user id

    private Map<String, Object> metricValues;
    private Long serverDate;

    private List<DataFlowDto> jobs;

    private Boolean stopping;
    private Boolean isDeleted;
    private Boolean deleted; // 冗余字段，flow engine 同时使用了 isDeleted 和 deleted 字段

    @JsonProperty("tm_user_id")
    private String tmUserId;


    private String updateStatus;

    private String updateMsg;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private Date updateTime;

    private String updateVersion;

    private Long updatePingTime;

    private String progres;

    @JsonProperty("worker_status")
    @Field("worker_status")
    private ApiServerStatus workerStatus;

    @JsonProperty("start_time")
    private Long startTime;

    private String gitCommitId;
    private String singletonLock; // 单例锁标记，每次启动都会更新
    private Boolean licenseBind;
    @JsonProperty("worker_date")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date workerDate;
}
