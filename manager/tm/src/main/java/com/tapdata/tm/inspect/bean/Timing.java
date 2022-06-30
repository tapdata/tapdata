package com.tapdata.tm.inspect.bean;

import lombok.Data;

/**
 * @Author: Zed
 * @Date: 2021/9/14
 * @Description:
 */
@Data
public class Timing {
    private Long intervals;

    /**
     * second
     * minute
     * hour
     * date
     * week
     * month
     */
    private String intervalsUnit;
    private Long start;
    private Long end;
}
