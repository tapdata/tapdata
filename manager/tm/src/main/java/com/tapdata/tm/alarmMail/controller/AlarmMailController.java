package com.tapdata.tm.alarmMail.controller;

import cn.hutool.core.lang.Validator;
import com.tapdata.tm.alarmMail.dto.AlarmMailDto;
import com.tapdata.tm.alarmMail.service.AlarmMailService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.UserDetail;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;

@RestController
@RequestMapping("/api/alarmMail")
@Setter(onMethod_ = {@Autowired})
public class AlarmMailController extends BaseController {
    private AlarmMailService alarmMailService;
    @Operation(summary = "Find the user’s default alarm recipient")
    @GetMapping
    public ResponseMessage<AlarmMailDto> findOne() {
        AlarmMailDto alarmMailDto = alarmMailService.findOne(new Filter(),getLoginUser());
        if(alarmMailDto == null){
            alarmMailDto = new AlarmMailDto();
            alarmMailDto.setEmailAddressList(new ArrayList<>());
        }
        return success(alarmMailDto);
    }

    @Operation(summary = "Add user default recipient")
    @PostMapping("/save")
    public ResponseMessage<Void> alarmMailSave(@RequestBody AlarmMailDto alarmMailDto) {
        UserDetail userDetail = getLoginUser();
        if(null == userDetail){
            return failed("AccessCode.No.User");
        }
        if(null != alarmMailDto && CollectionUtils.isNotEmpty(alarmMailDto.getEmailAddressList())){
            for(String address : alarmMailDto.getEmailAddressList()){
                if(!Validator.isEmail(address)) return failed("Email.Format.wrong");
            }
        }
        alarmMailService.upsert(new Query(),alarmMailDto, userDetail);
        return success();
    }
}
