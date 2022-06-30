package com.tapdata.tm.task.bean;

import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import lombok.Data;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2022/3/1
 * @Description:
 */
@Data
public class TranModelReqDto {
    private String nodeId;
    private List<DatabaseNode> nodes;
}
