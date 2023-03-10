package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.task.service.TaskConsoleService;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskResetLogService;
import com.tapdata.tm.task.vo.RelationTaskInfoVo;
import com.tapdata.tm.task.vo.RelationTaskRequest;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import com.tapdata.tm.task.vo.TaskLogInfoVo;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

@Tag(name = "复制dag信息输出")
@RestController
@RequestMapping("/api/task-console")
@Setter(onMethod_ = {@Autowired})
public class TaskConsoleController extends BaseController {

    private TaskDagCheckLogService taskDagCheckLogService;
    private TaskConsoleService taskConsoleService;
    private TaskResetLogService taskResetLogService;

    @GetMapping("")
    @Operation(summary = "信息输出日志接口")
    public ResponseMessage<TaskDagCheckLogVo> console(HttpServletRequest request,
              @Parameter(description = "任务id", required = true) @RequestParam String taskId,
              @Parameter(description = "节点id，默认空，查全部节点日志") @RequestParam(required = false) String nodeId,
              @Parameter(description = "搜索关键字") @RequestParam(required = false) String keyword,
              @Parameter(description = "日志等级 INFO WARN ERROR") @RequestParam(required = false) String grade,
              @Parameter(description = "日志类型, checkDag, reset") @RequestParam(required = false) String type,
              @Parameter(description = "保存还是启动 true 启动") @RequestParam(defaultValue = "false") boolean startTask) {
        TaskLogDto dto = new TaskLogDto();
        dto.setTaskId(taskId);
        dto.setNodeId(nodeId);
        dto.setKeyword(keyword);
        dto.setGrade(grade);
        dto.setStartTask(startTask);

        Locale locale = WebUtils.getLocale(request);
        if ("checkDag".equals(type)) {
            return success(taskDagCheckLogService.getLogs(dto, getLoginUser(), locale));
        } else if ("reset".equals(type)) {
            return success(taskResetLogService.getLogs(dto));
        } else {
            TaskDagCheckLogVo taskDagCheckLogVo = taskDagCheckLogService.getLogs(dto, getLoginUser(), locale);
            TaskDagCheckLogVo taskResetLogVo = taskResetLogService.getLogs(dto);
            LinkedHashMap<String, String> nodes = taskResetLogVo.getNodes();
            if (nodes == null) {
                nodes = new LinkedHashMap<>();
                taskDagCheckLogVo.setNodes(nodes);
            }
            nodes.putAll(taskResetLogVo.getNodes());

            LinkedList<TaskLogInfoVo> taskLogInfoVos = taskDagCheckLogVo.getList();
            if (CollectionUtils.isEmpty(taskLogInfoVos)) {
                taskLogInfoVos = new LinkedList<>();
                taskDagCheckLogVo.setList(taskLogInfoVos);
            }
            taskLogInfoVos.addAll(taskResetLogVo.getList());
            boolean over = taskDagCheckLogVo.isOver() && taskResetLogVo.isOver();
            taskDagCheckLogVo.setOver(over);

            // over=true => To prevent the front-end parallel request from getting old data
            if (over) {
                CompletableFuture.runAsync(() -> taskDagCheckLogService.removeAllByTaskId(taskId));
            }

            return success(taskDagCheckLogVo);
        }
    }

    @PostMapping("/relations")
    @Operation(summary = "可观测界面展示 关联任务列表")
    public ResponseMessage<List<RelationTaskInfoVo>> relation(@RequestBody RelationTaskRequest request) {
        return success(taskConsoleService.getRelationTasks(request));
    }
}
