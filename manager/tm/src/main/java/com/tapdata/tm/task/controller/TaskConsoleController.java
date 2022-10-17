package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.task.service.TaskConsoleService;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.vo.RelationTaskInfoVo;
import com.tapdata.tm.task.vo.RelationTaskRequest;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.print.attribute.ResolutionSyntax;
import java.util.List;

@Tag(name = "复制dag信息输出")
@RestController
@RequestMapping("/api/task-console")
@Setter(onMethod_ = {@Autowired})
public class TaskConsoleController extends BaseController {

    private TaskDagCheckLogService taskDagCheckLogService;
    private TaskConsoleService taskConsoleService;

    @GetMapping("")
    @Operation(summary = "信息输出日志接口")
    public ResponseMessage<TaskDagCheckLogVo> console(
            @Parameter(description = "任务id", required = true) @RequestParam String taskId,
            @Parameter(description = "节点id，默认空，查全部节点日志") @RequestParam(required = false) String nodeId,
            @Parameter(description = "搜索关键字") @RequestParam(required = false) String keyword,
            @Parameter(description = "日志等级 INFO WARN ERROR") @RequestParam(required = false) String grade) {
        TaskLogDto dto = new TaskLogDto();
        dto.setTaskId(taskId);
        dto.setNodeId(nodeId);
        dto.setKeyword(keyword);
        dto.setGrade(grade);
        return success(taskDagCheckLogService.getLogs(dto));
    }

    @PostMapping("/relations")
    @Operation(summary = "可观测界面展示 关联任务列表")
    public ResponseMessage<List<RelationTaskInfoVo>> relation(@RequestBody RelationTaskRequest request) {
        return success(taskConsoleService.getRelationTasks(request));
    }
}
