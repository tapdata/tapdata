package com.tapdata.tm.apiCalls.param;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class SaveApiCallParam {
    private String req_params;

    private Long res_rows;

    private Long latency;

    private Long reqTime;

    private Long resTime;

    private Map api_meta;

    private Map user_info;

    private String allPathId;

    private String api_path;

    private String api_name;

    private String call_id;

    private String user_ip;

    private List<String> user_ips;
    private String user_port;
    private String req_path;
    private String method;
    private String api_gateway_ip;
    private String api_gateway_port;
    private String api_worker_ip;
    private String api_worker_port;
    private String api_worker_uuid;
    private String api_gateway_uuid;

    private Map req_headers;

    private Long req_bytes;
}
