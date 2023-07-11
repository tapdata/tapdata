package com.tapdata.tm.clusterOperation.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class ClusterOperationDto extends BaseDto {
    private String type;
    private String uuid;

    private String process_id;

    private Date operationTime;
    private String downloadUrl;
    private List downloadList;
    private Integer status;
    private  String hostname;
    private String server;
    private String operation;

    private String token;

    private String onlyUpdateToken;

}
