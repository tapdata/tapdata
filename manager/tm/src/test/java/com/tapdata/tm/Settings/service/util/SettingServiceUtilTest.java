package com.tapdata.tm.Settings.service.util;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.tm.Settings.dto.SettingsDto;
import com.tapdata.tm.Settings.entity.Settings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SettingServiceUtilTest {
    @Nested
    class CopyPropertiesTest {
        String decode;
        List<Settings> settingsList;
        Settings settings;
        @BeforeEach
        void init() {
            settings = mock(Settings.class);
            decode = "2";
            settingsList = new ArrayList<>();
            settingsList.add(settings);

            when(settings.getKey()).thenReturn("smtp.server.password");
            doNothing().when(settings).setValue("*****");
        }

        void assertVerify(String decodeTemp,List<Settings> settingsListTemp, Settings settingTemp,
                          int copyPropertiesTimes, int getKeyTimes, int setValueTimes) {
            try (MockedStatic<BeanUtil> mockedStatic = mockStatic(BeanUtil.class);
            ) {
                mockedStatic.when(() -> BeanUtil.copyProperties(settingTemp, SettingsDto.class)).thenAnswer(w->null);
                List<SettingsDto> dtos = SettingServiceUtil.copyProperties(decodeTemp, settingsListTemp);
                Assertions.assertNotNull(dtos);
                Assertions.assertEquals(ArrayList.class, dtos.getClass());
                mockedStatic.verify(() -> BeanUtil.copyProperties(settingTemp, SettingsDto.class), times(copyPropertiesTimes));
            } finally {
                verify(settings, times(getKeyTimes)).getKey();
                verify(settings, times(setValueTimes)).setValue("*****");
            }
        }
        @Test
        void normal() {
            assertVerify(decode, settingsList, settings, 1, 1, 1);
        }
        @Test
        void emptySettingsList() {
            List<Settings> setting = new ArrayList<>();
            assertVerify(decode, setting, settings, 0, 0, 0);
        }
        @Test
        void nullSettingsList() {
            assertVerify(decode, null, settings, 0, 0, 0);
        }
        @Test
        void decodeWith1() {
            decode = "1";
            assertVerify(decode, settingsList, settings, 1, 0, 0);
        }
        @Test
        void getKeyWithOtherString() {
            when(settings.getKey()).thenReturn("smtp.server.password1");
            assertVerify(decode, settingsList, settings, 1, 1, 0);
        }
    }
}