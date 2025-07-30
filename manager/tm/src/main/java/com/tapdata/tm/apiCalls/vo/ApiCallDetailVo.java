package com.tapdata.tm.apiCalls.vo;

import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper=false)
public class ApiCallDetailVo extends BaseVo {
    private String apiId;
    private String name;
    private  String apiPath;
    private  String code;
    private Long visitTotalCount;
    private Long latency;
    private String reqParams;
    private Date createTime;
    private String codeMsg;
    private String method;
    private Long speed;
    private Long averResponseTime;
    private String clientName;
    private String userIp;
    private String query;
    private String body;
    private Map<String, Object> reqHeaders;
}
