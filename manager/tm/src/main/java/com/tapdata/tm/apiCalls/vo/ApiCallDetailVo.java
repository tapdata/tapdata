package com.tapdata.tm.apiCalls.vo;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.commons.base.DecimalFormat;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper=false)
public class ApiCallDetailVo extends BaseVo {
    private String apiId;
    private String name;
    private  String apiPath;
    private  String code;
    private Long visitTotalCount;
    @DecimalFormat(maxScale = 1, scale = 1)
    private Double latency;
    @DecimalFormat(maxScale = 1, scale = 1)
    private Double dbCost;
    private String reqParams;
    private Date createTime;
    private Date reqTime;
    private String codeMsg;
    private String method;
    private Double speed;
    private Long averResponseTime;
    private String clientName;
    private String userIp;
    private String query;
    private String body;
    private Map<String, Object> reqHeaders;
    private boolean failed;

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
    @DecimalFormat(maxScale = 1, scale = 1)
    private Number dataQueryTotalTime;

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
     * @see ApiCallEntity.HttpStatusType
     * */
    private String httpStatus;

    private Map<String, List<String>> fieldEncryptionRule;

    /**
     * Request start time
     * */
    private Long callStart;
    /**
     * Request end time
     * */
    private Long callEnd;
    /**
     * response byte count
     * */
    private Long responseBytes;
    /**
     * Database response rate = response byte count / dataQueryTotalTime
     * unit: B/s
     * */
    @DecimalFormat(maxScale = 4, scale = 1)
    private Double dbRate;
    /**
     * http response time
     * unit: ms
     * */
    @DecimalFormat(maxScale = 4, scale = 1)
    private Double httpTime;
}
