package com.tapdata.tm.task.bean;

import com.tapdata.tm.utils.Lists;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Zed
 * @Date: 2022/3/24
 * @Description:
 */
@Data
public class TaskOpResp {
    private List<String> successIds;

    public TaskOpResp() {
        this.successIds = new ArrayList<>();
    }

    public TaskOpResp(String taskId) {
        this.successIds = Lists.of(taskId);
    }

    public TaskOpResp(List<String> taskIds) {
        this.successIds = taskIds;
    }
}
