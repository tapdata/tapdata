package com.tapdata.tm.cluster.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class RawServerInfo {
    private Long pid;
    private Long createTime;
    private Double cpuPercent;
    private Double memoryPercent;
    private String state;
    private String msg;
}
