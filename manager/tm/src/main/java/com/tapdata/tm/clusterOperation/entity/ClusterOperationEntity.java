package com.tapdata.tm.clusterOperation.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;

@Document("ClusterOperation")
@Data
@EqualsAndHashCode(callSuper=false)
public class ClusterOperationEntity extends BaseEntity {
    private String type;
    private String uuid;

    private String process_id;

    private Date operationTime;
    private String downloadUrl;
    private List<?> downloadList;
    private Integer status;
    private  String hostname;
    private String server;
    private String operation;
    private String token;

}
