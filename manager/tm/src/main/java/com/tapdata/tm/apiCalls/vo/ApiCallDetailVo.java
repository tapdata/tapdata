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
    private Double speed;
    private Long averResponseTime;
    private String clientName;
    private String userIp;
    private String query;
    private String body;
    private Map<String, Object> reqHeaders;

    /**The unique identifier of the worker corresponding to the current request*/
    private String workOid;

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
}
