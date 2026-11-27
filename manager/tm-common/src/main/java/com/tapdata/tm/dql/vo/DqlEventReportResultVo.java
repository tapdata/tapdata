package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;

@Data
public class DqlEventReportResultVo implements Serializable {
    private String eventId;
    private String status;
    private boolean duplicate;
}
