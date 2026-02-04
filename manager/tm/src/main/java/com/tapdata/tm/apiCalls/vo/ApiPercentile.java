package com.tapdata.tm.apiCalls.vo;

import com.tapdata.tm.commons.base.DecimalFormat;
import lombok.Data;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/4 14:16 Create
 * @description
 */
@Data
public class ApiPercentile {
    private String apiId;
    private String name;
    /**
     * percentile 50, millisecond
     * */
    @DecimalFormat(scale = 1, maxScale = 1)
    private Double p50;
    /**
     * percentile 95, millisecond
     * */
    @DecimalFormat(scale = 1, maxScale = 1)
    private Double p95;
    /**
     * percentile 99, millisecond
     * */
    @DecimalFormat(scale = 1, maxScale = 1)
    private Double p99;
}
