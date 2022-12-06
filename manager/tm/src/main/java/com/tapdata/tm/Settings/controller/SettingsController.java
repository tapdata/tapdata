package com.tapdata.tm.Settings.controller;

import com.tapdata.tm.Settings.dto.TestMailDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.param.EnterpriseUpdateParam;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarmrule.dto.UpdateRuleDto;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Setter;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/api/Settings")
@Setter(onMethod_ = {@Autowired})
public class SettingsController extends BaseController {
    private SettingsService settingsService;
    private AlarmSettingService alarmSettingService;

    /**
     * flowEgine启动时候调用
     *
     * @param decode
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage find(@RequestParam(value = "decode", required = false) String decode,
                                @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        List<SettingsDto> settingList = settingsService.findALl(decode, filter);
        return success(settingList);
    }

    @Operation(summary = "Find a setting by {{id}} from the data source")
    @GetMapping("/{id}")
    public ResponseMessage<Settings> findById(@PathVariable("id") String id) {
        return success(settingsService.findById(id));
    }

    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("/update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
        Where where = parseWhere(whereJson);
        Document body = Document.parse(reqBody);
        if (!body.containsKey("$set") && !body.containsKey("$setOnInsert") && !body.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", body);
            body = _body;
        }
        long count = settingsService.updateByWhere(where, body);
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    @Operation(summary = "企业版修改配置")
    @PostMapping("/enterpriseUpdate")
    public ResponseMessage enterpriseUpdate(@RequestParam("where") String whereJson, @RequestBody EnterpriseUpdateParam enterpriseUpdateParam) {
        Where where = parseWhere(whereJson);
        Long count = settingsService.enterpriseUpdate(where, JsonUtil.toJson(enterpriseUpdateParam));
        Map retMap = new HashMap();
        retMap.put("count", count);
        return success(retMap);
    }

    @Operation(summary = "Settings save")
    @PatchMapping("/save")
    public ResponseMessage<Void> save(@RequestBody List<SettingsDto> settingsDto) {
        settingsService.save(settingsDto);
        return success();
    }

    @Operation(summary = "alarm save")
    @PostMapping("/alarm_save")
    public ResponseMessage<Void> alarmSave(@RequestBody List<AlarmSettingDto> alarms) {
        alarmSettingService.save(alarms);
        return success();
    }

    @Operation(summary = "find all alarms")
    @GetMapping("/alarm_find")
    public ResponseMessage<List<AlarmSettingDto>> findAllAlarmList() {
        return success(alarmSettingService.findAll());
    }

    @Operation(summary = "update rule by key")
    @PostMapping("/alarm_update")
    public ResponseMessage<Void> updateAlarm(@RequestBody UpdateRuleDto ruleDto) {
        alarmSettingService.updateSystemNotify(ruleDto);

        return success();
    }


    @Operation(summary = "test send mail")
    @PostMapping("testEmail")
    public ResponseMessage<Void> testSendMail(@RequestBody TestMailDto testMailDto) {
        settingsService.testSendMail(testMailDto);
        return success();
    }
}
