package com.tapdata.tm.modules.dto;

import com.tapdata.tm.module.dto.PathSetting;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PathSettingTest {

    @Nested
    class ofTest {
        @Test
        void test_main() {
            PathSetting pathSetting = PathSetting.of(PathSetting.PathSettingType.DEFAULT_POST);
            Assertions.assertEquals(PathSetting.PathSettingType.DEFAULT_POST, pathSetting.getType());
            Assertions.assertEquals(PathSetting.PathSettingType.DEFAULT_POST.getDefaultPath(), pathSetting.getPath());
            Assertions.assertEquals(PathSetting.PathSettingType.DEFAULT_POST.getMethod(), pathSetting.getMethod());
        }
    }
}