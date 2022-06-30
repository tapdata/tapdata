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

    private List<CustomMonitorInfo> customMonitor;

//    @Data
//    public static class Component {
//        private String status;
//        private String processID;
//    }

}
