package com.tapdata.tm.task.bean;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author: Zed
 * @Date: 2022/2/21
 * @Description:
 */
@Data
@AllArgsConstructor
public class LogSystemConfigDto {
    private String persistenceMode;
    private String persistenceMemory_size;
    private String persistenceMongodb_uri_db;
    private String persistenceMongodb_collection;
    private String persistenceRocksdb_path;
    private String share_cdc_ttl_day;

}
