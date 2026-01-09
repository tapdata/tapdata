package com.tapdata.tm.worker.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.springframework.data.mongodb.core.mapping.Document;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("ServerUsage")
public class ServerUsage extends BaseEntity {

    /**
     * 0: 5S颗粒度点位
     * 1: 1分钟颗粒度点位
     * 2: 1小时颗粒度点位
     * null: 零散点位
     */
    protected Integer type;
    /**
     * 0:tm
     * 1:engine
     * 2:api server
     * 3:api server worker
     */
    protected int processType;
    /**
     * processType=0, processId=tm id
     * processType=1, processId=engine id
     * processType=2, processId=api server id
     * processType=3, processId=api server id
     */
    protected String processId;

    /**
     * 只有当processType=3时，workOid=worker id
     */
    protected String workOid;

    protected Double cpuUsage;

    protected Long heapMemoryMax;

    protected Long heapMemoryUsage;

    protected Long lastUpdateTime;

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