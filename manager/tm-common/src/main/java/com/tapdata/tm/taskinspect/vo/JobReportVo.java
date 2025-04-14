package com.tapdata.tm.taskinspect.vo;

import com.tapdata.tm.taskinspect.cons.JobStatus;

import java.util.HashMap;
import java.util.Map;

/**
 * 运行记录-状态上报参数
 * <p>
 * 需要选择性更新，所以用 MAP 处理会比较好
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/25 10:06 Create
 */
public class JobReportVo extends HashMap<String, Object> {
    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_MESSAGE = "message";
    public static final String FIELD_CONFIG = "config";
    public static final String FIELD_ATTRS = "attrs";

    public Object getTimestamp() {
        return get(FIELD_TIMESTAMP);
    }

    public Object getStatus() {
        return get(FIELD_STATUS);
    }

    public Object getMessage() {
        return get(FIELD_MESSAGE);
    }

    public Object getConfig() {
        return get(FIELD_CONFIG);
    }

    public Object getAttrs() {
        return get(FIELD_ATTRS);
    }

    public JobReportVo message(String message) {
        put(FIELD_MESSAGE, message);
        return this;
    }

    public JobReportVo config(Object config) {
        put(FIELD_CONFIG, config);
        return this;
    }

    public JobReportVo attrs(Map<String, Object> attrs) {
        put(FIELD_ATTRS, attrs);
        return this;
    }

    public static JobReportVo create(JobStatus status) {
        JobReportVo vo = new JobReportVo();
        vo.put(FIELD_TIMESTAMP, System.currentTimeMillis());
        vo.put(FIELD_STATUS, status);
        return vo;
    }
}
