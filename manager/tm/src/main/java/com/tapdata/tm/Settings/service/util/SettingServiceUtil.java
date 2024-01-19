package com.tapdata.tm.Settings.service.util;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.entity.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SettingServiceUtil {
    public static List<SettingsDto> copyProperties(String decode, List<Settings> settingsList) {
        boolean needDecode = !"1".equals(decode);
        List<SettingsDto> settingsDtoList = new ArrayList<>();
        if (null == settingsList || settingsList.isEmpty()) return settingsDtoList;
        settingsList.stream()
                .filter(Objects::nonNull)
                .forEach(settings -> {
            if (needDecode && "smtp.server.password".equals(settings.getKey())) {
                settings.setValue("*****");
            }
            SettingsDto settingsDto = BeanUtil.copyProperties(settings, SettingsDto.class);
            settingsDtoList.add(settingsDto);
        });
        return settingsDtoList;
    }
}
