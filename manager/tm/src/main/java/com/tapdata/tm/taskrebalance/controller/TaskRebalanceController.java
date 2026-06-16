package com.tapdata.tm.taskrebalance.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.taskrebalance.service.TaskRebalanceService;
import com.tapdata.tm.taskrebalance.vo.TaskRebalanceDetailVo;
import com.tapdata.tm.taskrebalance.vo.TaskRebalancePreviewVo;
import com.tapdata.tm.taskrebalance.vo.TaskRebalanceVo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Map;

@Tag(name = "TaskRebalance", description = "Task rebalance APIs")
@RestController
@RequestMapping("/api/task/rebalance")
public class TaskRebalanceController extends BaseController {
    private final TaskRebalanceService taskRebalanceService;

    public TaskRebalanceController(TaskRebalanceService taskRebalanceService) {
        this.taskRebalanceService = taskRebalanceService;
    }

    @Operation(summary = "Generate task rebalance preview")
    @PostMapping("/preview")
    public ResponseMessage<TaskRebalancePreviewVo> preview() {
        return success(taskRebalanceService.preview(getLoginUser()));
    }

    @Operation(summary = "Create and execute task rebalance from preview")
    @PostMapping
    public ResponseMessage<TaskRebalanceVo> create(@RequestBody TaskRebalancePreviewVo preview) {
        return success(taskRebalanceService.createAndExecute(preview, getLoginUser()));
    }

    @Operation(summary = "Find task rebalance history")
    @GetMapping
    public ResponseMessage<Page<TaskRebalanceVo>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        return success(taskRebalanceService.findHistory(filter, getLoginUser()));
    }

    @Operation(summary = "Check active task rebalance")
    @GetMapping("/active")
    public ResponseMessage<Map<String, Boolean>> active() {
        return success(Collections.singletonMap("active", taskRebalanceService.hasActive(getLoginUser())));
    }

    @Operation(summary = "Find task rebalance detail")
    @GetMapping("/{id}")
    public ResponseMessage<TaskRebalanceDetailVo> detail(@PathVariable("id") String id) {
        return success(taskRebalanceService.detail(id, getLoginUser()));
    }

    @Operation(summary = "Cancel pending jobs in task rebalance")
    @PostMapping("/{id}/cancel")
    public ResponseMessage<Void> cancel(@PathVariable("id") String id) {
        taskRebalanceService.cancel(id, getLoginUser());
        return success();
    }

    @Operation(summary = "Cancel a pending task rebalance job")
    @PostMapping("/{id}/cancel/{taskId}")
    public ResponseMessage<Void> cancelJob(@PathVariable("id") String id, @PathVariable("taskId") String taskId) {
        taskRebalanceService.cancelJob(id, taskId, getLoginUser());
        return success();
    }
}
