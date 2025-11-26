package com.tapdata.tm.apiCalls.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;
import java.util.Map;



@EqualsAndHashCode(callSuper = true)
@Data
public class ApiCallDto extends BaseDto {
    @Field("req_params")
    private String reqParams;

    @JsonProperty("res_rows")
    @Field("res_rows")
    private Long resRows;

    private Long latency;

    private Long reqTime;

    private Long resTime;

    @JsonProperty("api_meta")
    private Map apiMeta;

    @JsonProperty("user_info")
    private Map userInfo;

    private String allPathId;

    @JsonProperty("api_path")
    private String apiPath;

    @JsonProperty("api_name")
    private String apiName;

    @JsonProperty("call_id")
    private String callId;

    @JsonProperty("user_ip")
    private String userIp;

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

    @JsonProperty("req_headers")
    private Map reqHeaders;

    @JsonProperty("req_bytes")
    @Field("req_bytes")
    private Long reqBytes;

    private String code;
    private String codeMsg;

    @JsonProperty("report_time")
    private Long reportTime;

    @JsonProperty("query")
    private String query;

    @JsonProperty("body")
    private String body;

    @JsonProperty("requestHeaders")
    private String requestHeaders;

    /**The unique identifier of the worker corresponding to the current request*/
    private String workOid;

    private Boolean supplement;


    /**
     * Start time of database access for API request
     * */
    private Long dataQueryFromTime;
    /**
     * End time of database access for API request
     * */
    private Long dataQueryEndTime;
    /**
     * Database access time for API requests
     * */
    private Long dataQueryTotalTime;
}
