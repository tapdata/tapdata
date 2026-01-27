package com.tapdata.tm.apiCalls.entity;

import com.tapdata.tm.base.field.CollectionField;

public enum ApiCallField implements CollectionField {
    REQ_PARAMS("req_params"),
    QUERY("query"),
    BODY("body"),
    RES_ROWS("res_rows"),
    LATENCY("latency"),
    REQ_TIME("reqTime"),
    RES_TIME("resTime"),
    API_META("api_meta"),
    USER_INFO("user_info"),
    ALL_PATH_ID("allPathId"),
    API_PATH("api_path"),
    API_NAME("api_name"),
    CALL_ID("call_id"),
    USER_IP("user_ip"),
    USER_IPS("user_ips"),
    USER_PORT("user_port"),
    REQ_PATH("req_path"),
    METHOD("method"),
    API_GATEWAY_IP("api_gateway_ip"),
    API_GATEWAY_PORT("api_gateway_port"),
    API_WORKER_IP("api_worker_ip"),
    API_WORKER_PORT("api_worker_port"),
    API_WORKER_UUID("api_worker_uuid"),
    API_GATEWAY_UUID("api_gateway_uuid"),
    REQ_HEADERS("req_headers"),
    REQ_BYTES("req_bytes"),
    CODE("code"),
    CODE_MSG("codeMsg"),
    REPORT_TIME("report_time"),
    WORK_O_ID("workOid"),
    SUPPLEMENT("supplement"),
    METRIC_COMPLETE("metricComplete"),
    DATA_QUERY_FROM_TIME("dataQueryFromTime"),
    DATA_QUERY_END_TIME("dataQueryEndTime"),
    DATA_QUERY_TOTAL_TIME("dataQueryTotalTime"),
    QUERY_OF_COUNT("queryOfCount"),
    QUERY_OF_PAGE("queryOfPage"),
    TOTAL_ROWS("totalRows"),
    REQUEST_COST("requestCost"),
    HTTP_STATUS("httpStatus"),
    DELETE("delete");

    final String fieldName;

    ApiCallField(String name) {
        this.fieldName = name;
    }

    @Override
    public String field() {
        return this.fieldName;
    }
}
