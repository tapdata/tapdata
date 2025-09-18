package com.tapdata.tm.modules.dto;


import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.PathSetting;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ModulesDtoTest {


    @Nested
    class WithPathSettingIfNeedTest {
        @Test
        void testWithPathSettingIfNeed() {
            ModulesDto dto = new ModulesDto();
            dto.withPathSettingIfNeed();
            Assertions.assertNotNull(dto.getPathSetting());
            Assertions.assertEquals(PathSetting.DEFAULT_PATH_SETTING, dto.getPathSetting());

            ModulesDto dto1 = new ModulesDto();
            dto1.setPathSetting(PathSetting.DEFAULT_PATH_SETTING);
            dto1.withPathSettingIfNeed();
            Assertions.assertNotNull(dto.getPathSetting());
            Assertions.assertEquals(PathSetting.DEFAULT_PATH_SETTING, dto.getPathSetting());
        }
    }
}