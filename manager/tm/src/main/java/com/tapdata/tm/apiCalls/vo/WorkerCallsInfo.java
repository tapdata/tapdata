package com.tapdata.tm.apiCalls.vo;

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

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

    private Long latency;

    private String code;

    private Long reqTime;

    private Long resTime;
}
