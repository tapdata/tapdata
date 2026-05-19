package com.tapdata.tm.cluster.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.cluster.dto.CustomMonitorInfo;
import com.tapdata.tm.cluster.dto.SystemInfo;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * 数据源模型
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ClusterState")
public class ClusterStateEntity extends BaseEntity {

    private SystemInfo systemInfo;

    private Integer reportInterval;
    private Component engine;

    private Component management;

    private Component apiServer;
    private List<CustomMonitorInfo> customMonitorStatus;
    private String uuid;
    private String status;
    private Date insertTime;
    private Date ttl;

    private String agentName;

    private String custIp;

    /**
     * 最近一次 ClusterComponentStopService.setClusterStateComponentStopped 写入时刻。
     * 用于 statusInfo 异步执行器判定本任务是否携带过期快照
     * （receivedAt &lt;= componentStoppedAt 即过期，整条 upsert skip）。
     * 仅 setClusterStateComponentStopped 写；statusInfo 路径必须 doc.remove("componentStoppedAt") 防止抹掉。
     */
    private Date componentStoppedAt;

    private List<CustomMonitorInfo> customMonitor;

//    @Data
//    public static class Component {
//        private String status;
//        private String processID;
//    }

}
