package com.tapdata.tm.alarmMail.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import lombok.Data;

import java.util.List;

@Data
public class AlarmMailDto extends BaseDto {
    private NotifyEnum type;
    private List<String> emailAddressList;
}
