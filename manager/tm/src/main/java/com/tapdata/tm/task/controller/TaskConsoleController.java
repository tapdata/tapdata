package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.observability.dto.TaskLogDto;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@Tag(name = "复制dag信息输出")
@RestController
@RequestMapping("/api/task/console")
@Setter(onMethod_ = {@Autowired})
public class TaskConsoleController extends BaseController {

    private TaskDagCheckLogService taskDagCheckLogService;

    @GetMapping("")
    @Operation(summary = "信息输出日志接口")
    public ResponseMessage<TaskDagCheckLogVo> console(
            @Parameter(description = "任务id", required = true) @RequestParam String taskId,
            @Parameter(description = "节点id，默认空，查全部节点日志") @RequestParam String nodeId,
            @Parameter(description = "搜索关键字") @RequestParam String keyword,
            @Parameter(description = "日志等级 INFO WARN ERROR") @RequestParam String grade) {
        TaskLogDto dto = new TaskLogDto();
        dto.setTaskId(taskId);
        dto.setNodeId(nodeId);
        dto.setKeyword(keyword);
        dto.setGrade(grade);
        return success(taskDagCheckLogService.getLogs(dto));
    }
}
