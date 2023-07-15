package io.tapdata.sybase.cdc.dto.start;

/**
 * @author GavinXiao
 * @description SybaseGeneralConfig create by Gavin
 * @create 2023/7/13 17:16
 **/
public class SybaseGeneralConfig {
    LivenessMonitor liveness_monitor;
    String license_path;
    String data_dir;
    String trace_dir;
    String error_connection_tracing;

    public LivenessMonitor getLiveness_monitor() {
        return liveness_monitor;
    }

    public void setLiveness_monitor(LivenessMonitor liveness_monitor) {
        this.liveness_monitor = liveness_monitor;
    }

    public String getLicense_path() {
        return license_path;
    }

    public void setLicense_path(String license_path) {
        this.license_path = license_path;
    }

    public String getData_dir() {
        return data_dir;
    }

    public void setData_dir(String data_dir) {
        this.data_dir = data_dir;
    }

    public String getTrace_dir() {
        return trace_dir;
    }

    public void setTrace_dir(String trace_dir) {
        this.trace_dir = trace_dir;
    }

    public String isError_connection_tracing() {
        return error_connection_tracing;
    }

    public void setError_connection_tracing(String error_connection_tracing) {
        this.error_connection_tracing = error_connection_tracing;
    }
}
