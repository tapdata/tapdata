package com.tapdata.tm.connectorRecord.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("ConnectorRecord")
@Data
@EqualsAndHashCode(callSuper=false)
public class ConnectorRecordEntity extends BaseEntity {
    private String pdkHash;
    private String pdkId;
    private String status;
    private String downloadSpeed;
    private String downFiledMessage;
    private String flag;
    private String connectionId;
    private Long fileSize;
    private Long progress;
    private String processId;
}
