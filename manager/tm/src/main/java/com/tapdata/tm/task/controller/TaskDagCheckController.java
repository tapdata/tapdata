package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.observability.dto.TaskLogDto;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "复制dag信息输出")
@RestController
@RequestMapping("/api/task-output")
@Setter(onMethod_ = {@Autowired})
public class TaskDagCheckController extends BaseController {

    @GetMapping("/log")
    public ResponseMessage<?> getLog(@Parameter(name = "dto", description = "日志dto", required = true)
                       @RequestBody TaskLogDto dto) {

        return success();
    }
}
