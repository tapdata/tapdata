package com.tapdata.tm.userLog.entity;

import com.tapdata.tm.audit.AuditBatch;
import com.tapdata.tm.audit.AuditChange;
import com.tapdata.tm.audit.AuditNode;
import java.util.Map;
import java.util.List;
import java.util.Date;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.userLog.dto.User;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("UserLogs") // 对应数据库表
public class UserLogs extends BaseEntity {
    public String ip;
    public String biz_module;
    public String desc;
    public String url;
    public User user;
    private String modular;
    private String operation;
    private String parameter1;
    private String parameter2;
    private String parameter3;
    private String modelName;

    private String messageId;
    private String username;
    private String type;
    //是否修改名称
    private Boolean rename;


    // ===== 统一审计事件字段（schemaVersion=2，历史记录缺失时按「不可用（历史记录）」展示）=====
    private Integer schemaVersion;
    private String eventId;
    private String operationId;
    private String phase;
    private String action;
    private String eventType;
    private String outcome;
    private String source;
    private String authType;
    private String attemptedAccount;
    private String credentialRef;
    private String sourceIp;
    private String sourceIpNote;
    private String forwardedFor;
    private String userAgent;
    private Date timestamp;
    private String objectType;
    private String objectId;
    private String objectName;
    private String reason;
    private String detailLevel;
    private String summary;
    private List<AuditChange> changes;
    private AuditBatch batch;
    private AuditNode node;
    private Map<String, String> context;

    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId sourceId;

}
