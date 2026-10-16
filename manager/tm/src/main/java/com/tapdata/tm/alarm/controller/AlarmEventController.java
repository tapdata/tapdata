package com.tapdata.tm.alarm.controller;

import com.tapdata.tm.alarm.dto.AlarmEventQuery;
import com.tapdata.tm.alarm.dto.AlarmEventStatsVo;
import com.tapdata.tm.alarm.dto.AlarmEventVo;
import com.tapdata.tm.alarm.service.AlarmEventQueryService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 告警事件（TAP-13052）。旧的 /api/alarm/list 保留兼容。
 */
@RestController
@RequestMapping("/api/alarms")
public class AlarmEventController extends BaseController {
    private final AlarmEventQueryService queryService;

    public AlarmEventController(AlarmEventQueryService queryService) {
        this.queryService = queryService;
    }

    @Operation(summary = "list alarm events")
    @GetMapping
    public ResponseMessage<Page<AlarmEventVo>> list(@ModelAttribute AlarmEventQuery query, HttpServletRequest request) {
        return success(queryService.list(query, getLoginUser(), WebUtils.getLocale(request)));
    }

    @Operation(summary = "alarm event statistics")
    @GetMapping("stats")
    public ResponseMessage<AlarmEventStatsVo> stats() {
        return success(queryService.stats(getLoginUser()));
    }

    @Operation(summary = "alarm event detail")
    @GetMapping("{id}")
    public ResponseMessage<AlarmEventVo> detail(@PathVariable("id") String id, HttpServletRequest request) {
        return success(queryService.detail(id, getLoginUser(), WebUtils.getLocale(request)));
    }
}
