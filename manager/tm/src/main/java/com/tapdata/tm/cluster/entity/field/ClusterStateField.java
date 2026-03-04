package com.tapdata.tm.cluster.entity.field;

import com.tapdata.tm.base.field.CollectionField;

/**
 * @see com.tapdata.tm.cluster.entity.ClusterStateEntity
 */
public enum ClusterStateField implements CollectionField {
    SYSTEM_INFO("systemInfo"),
    REPORT_INTERVAL("reportInterval"),
    ENGINE("engine"),
    MANAGEMENT("management"),
    API_SERVER("apiServer"),
    CUSTOM_MONITOR_STATUS("customMonitorStatus"),
    UUID("uuid"),
    STATUS("status"),
    INSERT_TIME("insertTime"),
    TTL("ttl"),
    AGENT_NAME("agentName"),
    CUSTER_IP("custIp"),
    CUSTOM_MONITOR("customMonitor");
    final String field;

    ClusterStateField(String field) {
        this.field = field;
    }


    @Override
    public String field() {
        return this.field;
    }

    public enum Engine implements CollectionField {
        STATUS(ClusterStateField.ENGINE.field.concat(".status")),
        PROCESS_ID(ClusterStateField.ENGINE.field.concat(".processID")),
        SERVER_ID(ClusterStateField.ENGINE.field.concat(".serverId")),
        SERVICE_STATUS(ClusterStateField.ENGINE.field.concat(".serviceStatus")),
        NET_STAT(ClusterStateField.ENGINE.field.concat(".netStat"));
        final String field;

        Engine(String field) {
            this.field = field;
        }


        @Override
        public String field() {
            return this.field;
        }
    }

    public enum Management implements CollectionField {
        STATUS(ClusterStateField.MANAGEMENT.field.concat(".status")),
        PROCESS_ID(ClusterStateField.MANAGEMENT.field.concat(".processID")),
        SERVER_ID(ClusterStateField.MANAGEMENT.field.concat(".serverId")),
        SERVICE_STATUS(ClusterStateField.MANAGEMENT.field.concat(".serviceStatus")),
        NET_STAT(ClusterStateField.MANAGEMENT.field.concat(".netStat"));
        final String field;

        Management(String field) {
            this.field = field;
        }


        @Override
        public String field() {
            return this.field;
        }
    }

    public enum ApiServer implements CollectionField {
        STATUS(ClusterStateField.API_SERVER.field.concat(".status")),
        PROCESS_ID(ClusterStateField.API_SERVER.field.concat(".processID")),
        SERVER_ID(ClusterStateField.API_SERVER.field.concat(".serverId")),
        SERVICE_STATUS(ClusterStateField.API_SERVER.field.concat(".serviceStatus")),
        NET_STAT(ClusterStateField.API_SERVER.field.concat(".netStat"));
        final String field;

        ApiServer(String field) {
            this.field = field;
        }


        @Override
        public String field() {
            return this.field;
        }
    }
}
