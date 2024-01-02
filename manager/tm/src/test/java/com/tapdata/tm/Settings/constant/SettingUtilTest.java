package com.tapdata.tm.Settings.constant;

import com.tapdata.tm.Settings.service.SettingsService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
@ExtendWith(MockitoExtension.class)
class SettingUtilTest {

    @Mock
    private SettingsService service;

    @Test
    void testGetValue() {
        SettingUtil settingUtil = new SettingUtil(service);
        when(service.getByCategoryAndKey(anyString(),anyString())).thenReturn("result");
        assertThat(SettingUtil.getValue("category", "key")).isEqualTo("result");
    }
}
