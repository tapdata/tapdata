package com.tapdata.tm.apiServer.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;


/**
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ApiServerDto extends BaseDto {
    private String clientName;
    private String clientURI;
    private String processId;
    private String clientSecret;

}
