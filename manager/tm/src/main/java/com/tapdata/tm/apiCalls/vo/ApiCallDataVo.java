package com.tapdata.tm.apiCalls.vo;

import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.vo.BaseVo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/7/29 12:20 Create
 * @description
 */
@Data
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@NoArgsConstructor
public class ApiCallDataVo {
    private ObjectId id;
    private String customId;

    private String apiId;

    private Date createAt;
    private Date lastUpdAt;
    private String userId;
    private String lastUpdBy;
    private String createUser;


    private Long latency;
    private Long reqTime;
    private Long resTime;
    private Map<String, Object> apiMeta;
    private Map<String, Object> userInfo;
    private String apiPath;
    private String callId;
    private String userIp;
    private List<String> userIps;
    private String userPort;
    private String reqPath;
    private String method;
    private String reqParams;
    private String query;
    private String body;

    private String apiGatewayIp;
    private String apiGatewayPort;
    private String apiWorkerIp;
    private String apiWorkerPort;
    private String apiWorkerUuid;
    private String apiGatewayUuid;

    private Map<String, Object> reqHeaders;
    private Long reqBytes;
    private String code;
    private String codeMsg;
    private Long reportTime;
    private Long visitTotalCount;
    private Date createTime;
    private Long speed;
    private Long averResponseTime;

    private String apiName;
    private String dataSource;
    private String tableName;
    private String apiVersion;
    private String basePath;
    private String readPreference;
    private String readPreferenceTag;
    private String readConcern;
    private String description;
    private String prefix;
    private String path;
    private String apiType;
    private String status;
    private List<Path> paths;
    private String project;
    private String createType;
    private String connectionId;
    private ObjectId connection;
    private String user;
    private String email;
    //请求失败率
    private Number failRate;
    private Long resRows;
    private Long visitCount;
    private Long responseTime;
    private Boolean isDeleted;
    private String operationType;
    private String connectionType;
    private String connectionName;
    /**
     * 访问路径方式  默认值 default  自定义 customize
     */
    private String pathAccessMethod;
    /**
     * 限制条数
     */
    private Integer limit;

    private Date apiCreateAt;
    private Date apiLastUpdAt;
    private String apiLastUpdBy;
    private String apiCreateUser;


    private String clientId;
    private String clientName;

    /**The unique identifier of the worker corresponding to the current request*/
    private String workOid;
}
