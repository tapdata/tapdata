package com.tapdata.tm.Settings.constant;

import com.tapdata.tm.Settings.service.SettingsService;
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
        return String.valueOf(settingsService.getByCategoryAndKey(category, key));
    }

}