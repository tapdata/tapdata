package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import com.tapdata.tm.task.service.TaskResetLogService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/TaskResetLogs")
public class TaskResetLogController extends BaseController {

    @Autowired
    private TaskResetLogService taskResetLogService;

    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<TaskResetEventDto> save(@RequestBody TaskResetEventDto taskResetEventDto) {
        taskResetEventDto.setId(null);
        return success(taskResetLogService.save(taskResetEventDto, getLoginUser()));
    }
}
