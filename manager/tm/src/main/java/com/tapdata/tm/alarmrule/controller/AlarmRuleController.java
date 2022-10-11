package com.tapdata.tm.alarmrule.controller;

import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@RestController
@RequestMapping("/api/alarm_rule")
@Setter(onMethod_ = {@Autowired})
public class AlarmRuleController extends BaseController {
    private AlarmRuleService alarmRuleService;

    @Operation(summary = "alarm rule save")
    @PostMapping("/save")
    public ResponseMessage<Void> alarmRuleSave(@RequestBody List<AlarmRuleDto> rules) {
        alarmRuleService.save(rules);
        return success();
    }

    @Operation(summary = "find all")
    @GetMapping("/find")
    public ResponseMessage<List<AlarmRuleDto>> find(){
        return success(alarmRuleService.findAll());
    }
}
