package com.tapdata.tm.monitor.dto;/**
 * Created by jiuyetx on 2022/8/12 18:14
 */

import lombok.Data;

/**
 * @author jiuyetx
 * @date 2022/8/12
 */
@Data
public class TableSyncStaticDto {
    private String taskRecordId;
    private Integer page;
    private Integer size;

    public Integer getSize() {
        return size > 100 ? 20 : size;
    }
}
