package com.tapdata.tm.utils;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageUtilDagCheckLocalizationTest {

    @Test
    void localizesDefaultNodeNameInEnglishDagCheckMessage() {
        String message = MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO", "数据源节点");

        assertTrue(message.contains("Data source node"));
    }

    @Test
    void preservesCustomNodeName() {
        String customName = "我的中文节点";

        assertEquals(customName, MessageUtil.localizeDagNodeName(Locale.US, customName));
    }

    @Test
    void keepsDefaultNodeNameForChineseLocale() {
        assertEquals("数据源节点", MessageUtil.localizeDagNodeName(Locale.CHINA, "数据源节点"));
    }
}
