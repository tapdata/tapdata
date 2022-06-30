package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.Date;

/**
 * @Author: Zed
 * @Date: 2022/2/23
 * @Description:
 */
@Data
public class LogCollectorStats {
    private Date time;
    private int inputNum;
    private int outputNum;
}
