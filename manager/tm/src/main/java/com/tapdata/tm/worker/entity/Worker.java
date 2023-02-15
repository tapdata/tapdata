package com.tapdata.tm.worker.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.worker.dto.TcmInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/10 下午3:30
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Workers")
public class Worker extends BaseEntity {

    @Field("process_id")
    private String processId;
    @Field("worker_type")
    private String workerType;
    private PlatformInfo platformInfo;
    private Integer weight;
    @Field("agentTags")
    private List<String> agentTags;
    @Field("tcm")
    private TcmInfo tcmInfo;
//    @Field("last_updated")
//    private Date lastUpdated;
    //private long createTime;
    @Field("ping_time")
    private Long pingTime;
    @Field("worker_ip")
    private String workerIp;
    private Integer cpuLoad;
    private String hostname;
    @Field("job_ids")
    private List<String> runningJobs;
    @Field("running_thread")
    private Integer runningThread;
    @Field("total_thread")
    private Integer totalThread;
    private Long usedMemory;
    private String version;
    @Field("accesscode")
    private String accessCode;

    private Map<String, Object> metricValues;

    private Boolean stopping;
    private Boolean isDeleted;
    private Boolean deleted;

    @Field("ping_date")
    @Indexed(name = "ping_date_index", expireAfterSeconds = 60 * 60)
    private Date pingDate = new Date();

    private Long startTime;

    private String gitCommitId;

    private String updateStatus;

    private String updateMsg;

    private Date updateTime;

    private String updateVersion;

    private Long updatePingTime;

    private String progres;


    @Field("worker_status")
    private Object worker_status;

    private String singletonLock; // 单例锁标记，每次启动都会更新
}
