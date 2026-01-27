package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/30 17:56 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ServerTopOnHomepage extends DataValueBase {

    Long totalRequestCount;

    Long errorCount;

    @DecimalFormat
    Double totalErrorRate;

    Long responseTime;

    Long notHealthyApiCount;

    Long notHealthyServerCount;

    public static ServerTopOnHomepage create() {
        ServerTopOnHomepage result = new ServerTopOnHomepage();
        result.setTotalRequestCount(0L);
        result.setTotalErrorRate(0.0D);
        result.setResponseTimeAvg(0.0D);
        result.setNotHealthyApiCount(0L);
        result.setNotHealthyServerCount(0L);
        result.setErrorCount(0L);
        return result;
    }

    public void merge(ApiMetricsRaw row) {
        if (null == row) {
            return;
        }
        Optional.ofNullable(row.getReqCount()).ifPresent(e -> setTotalRequestCount(getTotalRequestCount() + e));
        setTotalErrorRate(0.0D);
        setResponseTimeAvg(0.0D);
        setNotHealthyApiCount(0L);
        setNotHealthyServerCount(0L);
    }
}
