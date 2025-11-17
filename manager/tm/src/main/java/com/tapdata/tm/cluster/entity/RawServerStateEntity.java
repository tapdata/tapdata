package com.tapdata.tm.cluster.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.cluster.dto.RawServerInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("RawStateMetrix")
public class RawServerStateEntity extends BaseEntity {
    private RawServerInfo reportedData;
    private String dataSource;
    private String serviceId;
    private Date timestamp;
    private String serviceIP;
    private Integer servicePort;
    private Boolean delete;
}
