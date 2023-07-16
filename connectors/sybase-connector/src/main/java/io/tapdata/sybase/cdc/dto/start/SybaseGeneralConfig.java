package io.tapdata.sybase.cdc.dto.start;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @author GavinXiao
 * @description SybaseGeneralConfig create by Gavin
 * @create 2023/7/13 17:16
 **/
public class SybaseGeneralConfig implements ConfigEntity {
    LivenessMonitor liveness_monitor;
    String license_path;
    String data_dir;
    String trace_dir;
    String error_connection_tracing;
    String error_trace_dir;
    @Override
    public Object toYaml() {
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("liveness-monitor" , liveness_monitor.toYaml());
        map.put("license-path", license_path);
        map.put("data-dir", data_dir);
        map.put("trace-dir", trace_dir);
        //map.put("error-connection-tracing", error_connection_tracing);
        map.put("error-trace-dir", error_trace_dir);
        return map;
    }

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

    public String getError_connection_tracing() {
        return error_connection_tracing;
    }

    public String getError_trace_dir() {
        return error_trace_dir;
    }

    public void setError_trace_dir(String error_trace_dir) {
        this.error_trace_dir = error_trace_dir;
    }

}
