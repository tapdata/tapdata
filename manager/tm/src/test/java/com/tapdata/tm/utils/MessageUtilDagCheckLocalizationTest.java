package com.tapdata.tm.utils;

import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    @Test
    void localizesJsProcessorDefaultName() {
        assertEquals("Enhanced JS node", MessageUtil.localizeDagNodeName(Locale.US, "增强JS节点"));
    }

    @Test
    void returnsBlankNodeNameUnchanged() {
        assertEquals("", MessageUtil.localizeDagNodeName(Locale.US, ""));
        assertEquals("   ", MessageUtil.localizeDagNodeName(Locale.US, "   "));
        assertNull(MessageUtil.localizeDagNodeName(Locale.US, null));
    }

    @Test
    void localizeDagCheckParamsReturnsEmptyArrayInsteadOfNull() {
        Object[] localized = MessageUtil.localizeDagCheckParams(Locale.US, null);

        assertNotNull(localized);
        assertEquals(0, localized.length);
    }

    @Test
    void localizeDagCheckParamsKeepsEmptyArray() {
        Object[] localized = MessageUtil.localizeDagCheckParams(Locale.US, new Object[0]);

        assertNotNull(localized);
        assertEquals(0, localized.length);
    }

    @Test
    void localizeDagCheckParamsLocalizesStringsAndLeavesOtherTypes() {
        Date timestamp = new Date();
        Object[] localized = MessageUtil.localizeDagCheckParams(
                Locale.US,
                new Object[]{"数据源节点", timestamp, 3, "我的自定义节点"});

        assertArrayEquals(new Object[]{"Data source node", timestamp, 3, "我的自定义节点"}, localized);
    }

    @Test
    void getDagCheckMsgWithoutParamsStillReturnsTemplate() {
        String message = MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO");

        assertTrue(message.contains("Node {0} passes the check"));
    }
}
