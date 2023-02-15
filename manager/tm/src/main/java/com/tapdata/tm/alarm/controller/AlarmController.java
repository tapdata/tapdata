package com.tapdata.tm.alarm.controller;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.alarm.dto.AlarmListInfoVo;
import com.tapdata.tm.alarm.dto.AlarmListReqDto;
import com.tapdata.tm.alarm.dto.TaskAlarmInfoVo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.message.dto.MessageDto;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@RestController
@RequestMapping("/api/alarm")
@Setter(onMethod_ = {@Autowired})
@Slf4j
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
    @PostMapping("list_task")
    public ResponseMessage<TaskAlarmInfoVo> findListByTask(@RequestBody AlarmListReqDto dto) {
        return success(alarmService.listByTask(dto));
    }

    @Operation(summary = "close alarm")
    @PostMapping("close")
    public ResponseMessage<Void> close(@RequestParam String[] ids) {
        alarmService.close(ids, getLoginUser());
        return success();
    }

    /**
     * 目前只有agent的状态变动的时候，tcm调用该方法
     * @param messageDto
     * @return
     */
    @Operation(summary = "新增消息")
    @PostMapping("addMsg")
    public ResponseMessage<MessageDto> addMsg(@RequestBody MessageDto messageDto) {
        log.info("接收到新增信息请求  ,  messageDto:{}", JSON.toJSONString(messageDto));
        MessageDto messageDtoRet = alarmService.add(messageDto,getLoginUser());
        return success(messageDtoRet);
    }
}
