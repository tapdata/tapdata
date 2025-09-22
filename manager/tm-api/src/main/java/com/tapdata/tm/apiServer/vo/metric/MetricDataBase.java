package com.tapdata.tm.apiServer.vo.metric;

import lombok.Data;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 14:13 Create
 * @description
 */
@Data
public class MetricDataBase {

    /**
     * timestamp (ms)
     * */
    Long time;

    public MetricDataBase time(Long time) {
        this.time = time;
        return this;
    }

    public Object[] values() {
        return new Object[] {};
    }
}
