package com.tapdata.tm.deleteCaches.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;


/**
 * DeleteCaches
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DeleteCachesDto extends BaseDto {

    private String mongodbUri;
    private String collectionName;
    private Long timestamp;
    private Map<String, Object> data;

}