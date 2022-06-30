package com.tapdata.tm.task.bean;

import lombok.Builder;
import lombok.Data;

/**
 * @Author: Zed
 * @Date: 2022/3/5
 * @Description:
 */
@Data
@Builder
public class TransformWsResp {
    private Double progress;
    private int remainingTime;
    private int total;
    private int finished;
    private String status;
    private String stageId;
}
