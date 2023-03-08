package com.tapdata.tm.alarm.controller;

import com.tapdata.tm.alarm.dto.AlarmListInfoVo;
import com.tapdata.tm.alarm.dto.AlarmListReqDto;
import com.tapdata.tm.alarm.dto.TaskAlarmInfoVo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Locale;

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
                                                       @RequestParam(defaultValue = "20")Integer size,
                                                       HttpServletRequest request) {
        Locale locale = WebUtils.getLocale(request);
        return success(alarmService.list(status, start, end, keyword, page, size, getLoginUser(), locale));
    }

    @Operation(summary = "find all alarm by task")
    @PostMapping("list_task")
    public ResponseMessage<TaskAlarmInfoVo> findListByTask(@RequestBody AlarmListReqDto dto, HttpServletRequest request) {
        Locale locale = WebUtils.getLocale(request);
        dto.setLocale(locale);
        return success(alarmService.listByTask(dto));
    }

    @Operation(summary = "close alarm")
    @PostMapping("close")
    public ResponseMessage<Void> close(@RequestParam String[] ids) {
        alarmService.close(ids, getLoginUser());
        return success();
    }
}
