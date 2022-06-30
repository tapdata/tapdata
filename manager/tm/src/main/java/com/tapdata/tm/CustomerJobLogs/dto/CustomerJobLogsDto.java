package com.tapdata.tm.CustomerJobLogs.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.BasicBSONObject;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.Date;

@EqualsAndHashCode(callSuper = true)
@Data
@Document("CustomerJobLogs")
public class CustomerJobLogsDto extends BaseDto {

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