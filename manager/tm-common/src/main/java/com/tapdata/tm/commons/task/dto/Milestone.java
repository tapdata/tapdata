
package com.tapdata.tm.commons.task.dto;


import lombok.Data;

import java.io.Serializable;

@Data
public class Milestone implements Serializable {

    /**
     * code 可能的枚举值
     * INIT_CONNECTOR
     * INIT_TRANSFORMER
     * READ_SNAPSHOT    全量开始
     * WRITE_SNAPSHOT
     *
     * READ_CDC_EVENT   增量开始
     * WRITE_CDC_EVENT
     */
    private String code;
    private Long end;
    private String errorMessage;
    private Long start;
    private String status;
    private String syncStatus;
    private String group;
}
