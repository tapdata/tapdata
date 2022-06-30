package com.tapdata.tm.apiCalls.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;
import java.util.Map;


@EqualsAndHashCode(callSuper = true)
@Data
@Document("ApiCall")
public class ApiCallEntity extends BaseEntity {
    @Field("req_params")
    private String reqParams;

    //默认为0 避免出现空指针
    @Field("res_rows")
    private Long resRows=0L;

    private Long latency;

    private Long reqTime;

    private Long resTime;

    @Field("api_meta")
    private Map apiMeta;

    @Field("user_info")
    private Map userInfo;

    private String allPathId;

    @Field("api_path")
    private String apiPath;

    @Field("api_name")
    private String apiName;

    @Field("call_id")
    private String callId;

    @Field("user_ip")
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

    @Field("req_headers")
    private Map reqHeaders;

    @Field("req_bytes")
    private Long reqBytes;

    private String code;
    private String codeMsg;

    @Field("report_time")
    private Long reportTime;

}