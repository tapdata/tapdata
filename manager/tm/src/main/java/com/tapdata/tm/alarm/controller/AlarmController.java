package com.tapdata.tm.alarm.controller;

import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.dto.AlarmListInfoVo;
import com.tapdata.tm.alarm.dto.TaskAlarmInfoVo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.message.constant.Level;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@RestController
@RequestMapping("/api/alarm")
@Setter(onMethod_ = {@Autowired})
public class AlarmController extends BaseController {
    private AlarmService alarmService;

    @Operation(summary = "find all alarm")
    @GetMapping("list")
    public ResponseMessage<Page<AlarmListInfoVo>> list(@RequestParam(required = false)String status,
                                                       @RequestParam(required = false)Long start,
                                                       @RequestParam(required = false)Long end,
                                                       @RequestParam(required = false)String keyword,
                                                       @RequestParam(defaultValue = "1")Integer page,
                                                       @RequestParam(defaultValue = "20")Integer size) {
        return success(alarmService.list(status, start, end, keyword, page, size, getLoginUser()));
    }

    @Operation(summary = "find all alarm by task")
    @GetMapping("list_task")
    public ResponseMessage<TaskAlarmInfoVo> findListByTask(@RequestParam(required = false) AlarmStatusEnum status,
                                                       @RequestParam(required = false) Level level,
                                                       @RequestParam String taskId,
                                                       @RequestParam(required = false)String nodeId) {
        return success(alarmService.listByTask(status, level, taskId, nodeId, getLoginUser()));
    }

    @Operation(summary = "close alarm")
    @PostMapping("close")
    public ResponseMessage<Void> close(@RequestParam String[] ids) {
        alarmService.close(ids, getLoginUser());
        return success();
    }
}
