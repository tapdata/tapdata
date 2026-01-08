package com.tapdata.tm.v2.api.monitor.main.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:01 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ApiTopOnHomepage extends ValueBase {
    long apiCount;

    long totalRequestCount;

    long totalBytes;

    long totalDelayMs;

    double totalRps;

    double responseTimeAvg;
}
