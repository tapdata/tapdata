package com.tapdata.tm.webhook.dto;

import com.tapdata.tm.commons.alarm.AlarmComponentEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class WebHookAlterInfoDtoTest {
    @Test
    void testWithStatusTxt() {
        WebHookAlterInfoDto dto = new WebHookAlterInfoDto();
        dto.setComponent(AlarmComponentEnum.FE);
        dto.withComponentTxt(AlarmComponentEnum.FE);
        Assertions.assertEquals("引擎", dto.getComponentTxt());
    }
    @Test
    void testWithOtherStatusTxt() {
        WebHookAlterInfoDto dto = new WebHookAlterInfoDto();
        dto.withComponentTxt(null);
        Assertions.assertEquals("-", dto.getComponentTxt());
    }
}