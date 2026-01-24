package com.tapdata.tm.worker.dto;

import com.tapdata.tm.worker.entity.ServerUsage;
import lombok.Data;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/4 15:11 Create
 * @description
 */
@Data
public class MetricInfo {
    /**
     * unit byte
     * */
    Long heapMemoryUsage;

    Long heapMemoryUsageMax;

    /**
     * unit %, such as 0.1 means 10%
     * */
    Double cpuUsage;

    /**
     * only server main process usage
     * */
    Double selfCpuUsage;

    /**
     * timestamp, unit ms
     * */
    Object lastUpdateTime;

    public static ServerUsage toUsage(MetricInfo info, String processId, String workerId, ServerUsage.ProcessType type) {
        ServerUsage serverUsage = new ServerUsage();
        serverUsage.setProcessType(type.getType());
        serverUsage.setProcessId(processId);
        serverUsage.setWorkOid(Optional.ofNullable(workerId).orElse(""));
        serverUsage.setCpuUsage(Optional.ofNullable(info).map(MetricInfo::getCpuUsage).orElse(null));
        serverUsage.setHeapMemoryUsage(Optional.ofNullable(info).map(MetricInfo::getHeapMemoryUsage).orElse(null));
        serverUsage.setHeapMemoryMax(Optional.ofNullable(info).map(MetricInfo::getHeapMemoryUsageMax).orElse(null));
        serverUsage.setSelfCpuUsage(Optional.ofNullable(info).map(MetricInfo::getSelfCpuUsage).orElse(null));
        Object lastUpdateTime = Optional.ofNullable(info).map(MetricInfo::getLastUpdateTime).orElse(null);
        if (lastUpdateTime instanceof Number iTime) {
            serverUsage.setLastUpdateTime(iTime.longValue());
        } else if (lastUpdateTime instanceof Date iTime) {
            serverUsage.setLastUpdateTime(iTime.getTime());
        } else if (lastUpdateTime instanceof String iTime) {
            try {
                serverUsage.setLastUpdateTime(Long.parseLong(iTime));
            } catch (NumberFormatException e) {
                //ignore
            }
        }
        Optional.ofNullable(serverUsage.getLastUpdateTime()).ifPresent(t -> {
            long time = t / 1000L;
            if (time % 3600L == 0) {
                serverUsage.setType(2);
                return;
            }
            if (time % 60L  == 0) {
                serverUsage.setType(1);
                return;
            }
            if (time % 5L == 0L) {
                serverUsage.setType(0);
                return;
            }
            serverUsage.setType(null);
        });
        serverUsage.setCreateAt(new Date());
        serverUsage.setLastUpdAt(new Date());
        serverUsage.setId(new ObjectId());
        return serverUsage;
    }
}
