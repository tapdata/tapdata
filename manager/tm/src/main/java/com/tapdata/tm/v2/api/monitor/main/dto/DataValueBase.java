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
    @SortField(name = {"responseTimeAvg", "rta"}, originField = {"delay"})
    Double responseTimeAvg;

    @SortField(name = {"p95"}, originField = {"delay"})
    Long p95;

    @SortField(name = {"p99"}, originField = {"delay"})
    Long p99;

    @SortField(name = {"maxDelay"}, originField = {"delay"})
    Long maxDelay;

    @SortField(name = {"minDelay"}, originField = {"delay"})
    Long minDelay;

    @SortField(name = {"dbCostTotal"}, originField = {"dbCost"})
    long dbCostTotal;

    @DecimalFormat
    @SortField(name = {"dbCostAvg"}, originField = {"dbCost"})
    double dbCostAvg;

    @SortField(name = {"dbCostMax"}, originField = {"dbCost"})
    Long dbCostMax;

    @SortField(name = {"dbCostMin"}, originField = {"dbCost"})
    Long dbCostMin;

    @SortField(name = {"dbCostP95"}, originField = {"dbCost"})
    Long dbCostP95;

    @SortField(name = {"dbCostP99"}, originField = {"dbCost"})
    Long dbCostP99;
}
