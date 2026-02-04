package com.tapdata.tm.apiCalls.vo;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/2 17:06 Create
 * @description
 */
@Data
public class WorkerCallsInfo {
    @Field("workOid")
    private String workOid;

    @Field("api_gateway_uuid")
    private String apiGatewayUuid;

    @Field("allPathId")
    private String apiId;

    private Number latency;

    private String code;

    private String httpStatus;

    private Long reqTime;

    private Long resTime;

    @Field("req_path")
    private String reqPath;

    private Date lastCreateTime;

    private ObjectId lastApiCallId;

    private boolean failed;
}
