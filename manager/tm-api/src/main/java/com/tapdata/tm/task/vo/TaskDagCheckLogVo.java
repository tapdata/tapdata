package com.tapdata.tm.task.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedList;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "信息输出VO")
public class TaskDagCheckLogVo implements Serializable {
    @Schema(description = "节点名称")
    private LinkedHashMap<String, String> nodes;
    @Schema(description = "日志数据")
    private LinkedList<TaskLogInfoVo> list;
    @Schema(description = "模型推演日志数据")
    private LinkedList<TaskLogInfoVo> modelList;
    private int errorNum;
    private int warnNum;
    private boolean over;
}
