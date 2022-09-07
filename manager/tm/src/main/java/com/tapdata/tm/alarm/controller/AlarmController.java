package com.tapdata.tm.alarm.controller;

import com.tapdata.tm.alarm.dto.AlarmListInfoVo;
import com.tapdata.tm.alarm.dto.TaskAlarmInfoVo;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@RestController
@RequestMapping("/api/alarm")
@Setter(onMethod_ = {@Autowired})
public class AlarmController extends BaseController {

    @Operation(summary = "find all alarm")
    @GetMapping("/list")
    public ResponseMessage<Page<AlarmListInfoVo>> list(@RequestParam(required = false)String status,
                                                       @RequestParam(required = false)Long start,
                                                       @RequestParam(required = false)Long end,
                                                       @RequestParam(required = false)String keyword,
                                                       @RequestParam(required = false)Integer page,
                                                       @RequestParam(required = false)Integer size) {

        return success();
    }

    @Operation(summary = "find all alarm by task")
    @GetMapping("/list_task")
    public ResponseMessage<TaskAlarmInfoVo> findListByTask(@RequestParam(required = false)String status,
                                                       @RequestParam(required = false)String level,
                                                       @RequestParam(required = false)String taskId,
                                                       @RequestParam(required = false)String nodeId) {

        return success();
    }
}
