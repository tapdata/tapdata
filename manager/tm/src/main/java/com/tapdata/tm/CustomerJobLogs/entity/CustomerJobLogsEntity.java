package com.tapdata.tm.CustomerJobLogs.entity;

import com.tapdata.tm.CustomerJobLogs.dto.CustomerLogsLevel;
import com.tapdata.tm.base.entity.SchedulableEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.bson.BasicBSONObject;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("CustomerJobLogs")
@AllArgsConstructor
@NoArgsConstructor
public class CustomerJobLogsEntity extends SchedulableEntity {

    private CustomerLogsLevel level;
    private String key;
    private Long timestamp;
    private Date date;
    private String dataFlowId;
    private Integer version;
    private String searchKey;
    private BasicBSONObject params;
    private String template;
    private String link;
    private ArrayList<String> templateKeys;

}
