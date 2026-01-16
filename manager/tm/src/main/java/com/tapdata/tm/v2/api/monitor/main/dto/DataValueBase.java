package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import com.tapdata.tm.commons.base.SortField;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/16 21:54 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DataValueBase extends ValueBase {
    /**
     * 平均耗时
     * */
    @DecimalFormat
    @SortField(name = {"responseTimeAvg", "rta"})
    Double responseTimeAvg;

    @SortField(name = {"p95"})
    Long p95;

    @SortField(name = {"p99"})
    Long p99;

    @SortField(name = {"maxDelay"})
    Long maxDelay;

    @SortField(name = {"minDelay"})
    Long minDelay;

    @SortField(name = {"dbCostTotal"})
    long dbCostTotal;

    @DecimalFormat
    @SortField(name = {"dbCostAvg"})
    double dbCostAvg;

    @SortField(name = {"dbCostMax"})
    Long dbCostMax;

    @SortField(name = {"dbCostMin"})
    Long dbCostMin;

    @SortField(name = {"dbCostP95"})
    Long dbCostP95;

    @SortField(name = {"dbCostP99"})
    Long dbCostP99;
}
