package com.tapdata.tm.taskrebalance.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class TaskRebalanceDetailVo {
    private TaskRebalanceVo rebalance;
    private List<TaskRebalanceJobVo> jobs = new ArrayList<>();
}
