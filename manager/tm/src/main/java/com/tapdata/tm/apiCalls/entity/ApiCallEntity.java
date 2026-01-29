package com.tapdata.tm.apiCalls.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;
import java.util.Map;


@EqualsAndHashCode(callSuper = true)
@Data
@Document("ApiCall")
public class ApiCallEntity extends BaseEntity {
    @JsonProperty("req_params")
    @Field("req_params")
    private String reqParams;

    @Field("query")
    private String query;

    @Field("body")
    private String body;

    //默认为0 避免出现空指针
    @Field("res_rows")
    @JsonProperty("res_rows")
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
    private Map<String, Object> reqHeaders;

    @Field("req_bytes")
    private Long reqBytes;

    private String code;
    private String codeMsg;

    @Field("report_time")
    private Long reportTime;

    /**The unique identifier of the worker corresponding to the current request*/
    private String workOid;

    private Boolean supplement;

    /**
     * Has the current API request completed indicator statistics
     * Use secondary condition filtering during scheduled indicator statistics
     * */
    private Boolean metricComplete;

    /**
     * api请求的数据库访问开始时间
     * */
    private Long dataQueryFromTime;
    /**
     * api请求的数据库访问结束时间
     * */
    private Long dataQueryEndTime;
    /**
     * api请求的数据库访问耗时
     * */
    private Long dataQueryTotalTime;

    /**
     * query Of Count
     * */
    private String queryOfCount;

    /**
     * query Of Page
     * */
    private String queryOfPage;

    /**
     * query Of total count of table
     * */
    private Long totalRows;

    /**
     * query Of http request cost time(ms)
     * */
    private Long requestCost;

    /**
     * @see HttpStatusType
     * */
    private String httpStatus;

    private boolean succeed;

    private boolean delete;

    @Getter
    public enum HttpStatusType {
        API_NOT_EXIST_404("API_NOT_EXIST_404"),
        PUBLISH_FAILED_404("PUBLISH_FAILED_404"),
        API_NOT_PUBLISH_404("API_NOT_PUBLISH_404")
        ;
        final String code;

        HttpStatusType(String code) {
            this.code = code;
        }
    }
}