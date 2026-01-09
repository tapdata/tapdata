package com.tapdata.tm.v2.api.monitor.main.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/30 18:30 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TopApiInServer extends ValueBase {
    String apiId;
    String apiName;
    Long requestCount;
    Double errorRate;
    Double avg;
    Long p99;
    Long maxDelay;
    Long minDelay;

    public static TopApiInServer create() {
        TopApiInServer item = new TopApiInServer();
        item.setRequestCount(0L);
        item.setErrorRate(0.0D);
        item.setAvg(0.0D);
        return item;
    }
}
