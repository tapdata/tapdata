package com.tapdata.tm.utils;

import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageUtilDagCheckLocalizationTest {

    @Test
    void localizesDefaultNodeNameInEnglishDagCheckMessage() {
        String message = MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO",
                MessageUtil.dagNodeName("database", "数据源节点"));

        assertTrue(message.contains("Data source node"));
    }

    @Test
    void preservesCustomNodeName() {
        String customName = "我的中文节点";

        assertEquals(customName, MessageUtil.localizeDagNodeName(Locale.US, "database", customName));
    }

    @Test
    void keepsDefaultNodeNameForChineseLocale() {
        assertEquals("数据源节点", MessageUtil.localizeDagNodeName(Locale.CHINA, "database", "数据源节点"));
    }

    @Test
    void returnsMissingKeyInsteadOfJoiningParams() {
        assertEquals("MISSING_DAG_CHECK_KEY",
                MessageUtil.getDagCheckMsg(Locale.CHINA, "MISSING_DAG_CHECK_KEY", (Object) null));
    }

    @Test
    void formatsChineseUnionPassAndDoesNotThrowOnNullName() {
        String message = assertDoesNotThrow(
                () -> MessageUtil.getDagCheckMsg(Locale.CHINA, "UNION_PASS", (Object) null));
        assertTrue(message.contains("追加合并节点"));
        assertTrue(message.contains("检测通过"));
    }

    @Test
    void doesNotRewritePlainStringMatchingADefaultNodeName() {
        String message = MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO", "表节点");

        assertTrue(message.contains("表节点"));
        assertFalse(message.contains("Table node"));
    }

    @Test
    void preservesDefaultNameOfADifferentNodeType() {
        assertEquals("数据源节点", MessageUtil.localizeDagNodeName(Locale.US, "table", "数据源节点"));
    }

    @Test
    void localizesJsProcessorDefaultName() {
        assertEquals("Enhanced JS node", MessageUtil.localizeDagNodeName(Locale.US, "js_processor", "增强JS节点"));
    }

    @Test
    void returnsBlankNodeNameUnchanged() {
        assertEquals("", MessageUtil.localizeDagNodeName(Locale.US, "database", ""));
        assertEquals("   ", MessageUtil.localizeDagNodeName(Locale.US, "database", "   "));
        assertNull(MessageUtil.localizeDagNodeName(Locale.US, "database", null));
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
    void localizeDagCheckParamsLocalizesOnlyTypedNodeNames() {
        Date timestamp = new Date();
        Object[] localized = MessageUtil.localizeDagCheckParams(
                Locale.US,
                new Object[]{MessageUtil.dagNodeName("database", "数据源节点"), "数据源节点", timestamp, 3, "我的自定义节点"});

        assertArrayEquals(new Object[]{"Data source node", "数据源节点", timestamp, 3, "我的自定义节点"}, localized);
    }

    @Test
    void getDagCheckMsgWithoutParamsStillReturnsTemplate() {
        String message = MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO");

        assertTrue(message.contains("Node {0} passes the check"));
    }
}
