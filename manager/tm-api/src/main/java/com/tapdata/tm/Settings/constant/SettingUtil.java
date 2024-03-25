package com.tapdata.tm.Settings.constant;

import com.tapdata.tm.Settings.service.SettingsService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SettingUtil {
    private static SettingsService settingsService;

    @Autowired
    public SettingUtil(SettingsService service) {
        settingsService = service;
    }

    public static String getValue(String category, String key) {
        if(StringUtils.isBlank(category) || StringUtils.isBlank(key)) return null;
        Object value = settingsService.getByCategoryAndKey(category, key);
        return value != null ? String.valueOf(value):null ;
    }

}