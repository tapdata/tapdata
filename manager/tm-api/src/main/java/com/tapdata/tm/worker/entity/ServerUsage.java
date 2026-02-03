package com.tapdata.tm.worker.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.springframework.data.mongodb.core.mapping.Document;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("ServerUsage")
public class ServerUsage extends UsageBase {

    @Getter
    public enum ProcessType {
        TM(0),
        ENGINE(1),
        API_SERVER(2),
        API_SERVER_WORKER(3);
        final int type;

        ProcessType(int type) {
            this.type = type;
        }

        public static ProcessType as(int type) {
            for (ProcessType value : values()) {
                if (value.type == type) {
                    return value;
                }
            }
            return TM;
        }
    }


    public static ServerUsage instance(long ts, String processId, String workerId, int processType) {
        ServerUsage metric = new ServerUsage();
        metric.setLastUpdateTime(ts);
        metric.setProcessId(processId);
        metric.setWorkOid(workerId);
        metric.setProcessType(processType);
        return metric;
    }
}