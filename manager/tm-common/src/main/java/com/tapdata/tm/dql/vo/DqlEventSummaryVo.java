package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;

@Data
public class DqlEventSummaryVo implements Serializable {
    private long total;
    private long pending;
    private long reprocessing;
    private long recovered;
    private long recoveryFailed;
    private long notReprocessable;
}
