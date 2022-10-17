package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskResetLogService;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import com.tapdata.tm.task.vo.TaskLogInfoVo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.LinkedList;

@Tag(name = "复制dag信息输出")
@RestController
@RequestMapping("/api/task-console")
@Setter(onMethod_ = {@Autowired})
public class TaskConsoleController extends BaseController {

    private TaskDagCheckLogService taskDagCheckLogService;
    private TaskResetLogService taskResetLogService;

    @GetMapping("")
    @Operation(summary = "信息输出日志接口")
    public ResponseMessage<TaskDagCheckLogVo> console(
            @Parameter(description = "任务id", required = true) @RequestParam String taskId,
            @Parameter(description = "节点id，默认空，查全部节点日志") @RequestParam(required = false) String nodeId,
            @Parameter(description = "搜索关键字") @RequestParam(required = false) String keyword,
            @Parameter(description = "日志等级 INFO WARN ERROR") @RequestParam(required = false) String grade,
            @Parameter(description = "日志类型, checkDag, reset") @RequestParam(required = false) String type) {
        TaskLogDto dto = new TaskLogDto();
        dto.setTaskId(taskId);
        dto.setNodeId(nodeId);
        dto.setKeyword(keyword);
        dto.setGrade(grade);

        if ("checkDag".equals(type)) {
            return success(taskDagCheckLogService.getLogs(dto));
        } else if ("reset".equals(type)) {
            return success(taskResetLogService.getLogs(dto));
        } else {
            TaskDagCheckLogVo taskDagCheckLogVo = taskDagCheckLogService.getLogs(dto);
            TaskDagCheckLogVo taskResetLogVo = taskResetLogService.getLogs(dto);
            LinkedHashMap<String, String> nodes = taskResetLogVo.getNodes();
            if (nodes == null) {
                nodes = new LinkedHashMap<>();
            }
            nodes.putAll(taskResetLogVo.getNodes());

            LinkedList<TaskLogInfoVo> taskLogInfoVos = taskDagCheckLogVo.getList();
            if (CollectionUtils.isEmpty(taskLogInfoVos)) {
                taskLogInfoVos = new LinkedList<>();
            }
            taskLogInfoVos.addAll(taskResetLogVo.getList());
            return success(taskDagCheckLogVo);
        }
    }
}
