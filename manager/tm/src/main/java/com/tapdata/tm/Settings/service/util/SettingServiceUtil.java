package com.tapdata.tm.Settings.service.util;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.entity.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SettingServiceUtil {
    public static List<SettingsDto> copyProperties(String decode, List<Settings> settingsList) {
        List<SettingsDto> settingsDtoList = new ArrayList<>();
        if (null == settingsList || settingsList.isEmpty()) return settingsDtoList;
        settingsList.stream().filter(Objects::nonNull).forEach(settings -> {
            if (!"1".equals(decode)){
                if("smtp.server.password".equals(settings.getKey())) {
                    settings.setValue("*****");
                }
            } else {
                //todo 解密方法
                //settingsList.stream().filter(settings -> {
                //    if ("smtp.server.password".equals(settings.getKey())) {
                //        settings.setValue(EncrptAndDencryUtil.Decrypt(String.valueOf(settings.getValue())));
                //        return true;
                //    }
                //    return false;
                //}).collect(Collectors.toList());
            }
            SettingsDto settingsDto = BeanUtil.copyProperties(settings, SettingsDto.class);
            settingsDtoList.add(settingsDto);
        });
        return settingsDtoList;
    }
}
