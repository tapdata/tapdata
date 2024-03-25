package com.tapdata.tm.inspect.bean;

import lombok.Data;

/**
 * @Author: Zed
 * @Date: 2021/9/14
 * @Description:
 */
@Data
public class Limit {
    private Integer keep;
    private Integer fullMatchKeep;

    private String action;
}
