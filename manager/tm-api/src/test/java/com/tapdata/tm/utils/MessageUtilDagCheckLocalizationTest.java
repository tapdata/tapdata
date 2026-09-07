package com.tapdata.tm.utils;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageUtilDagCheckLocalizationTest {

    @Nested
    class GetDagCheckMsgTest {
        @Test
        void localizesDefaultNodeNameInEnglishMessage() {
            String message = MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO", "数据源节点");

            assertTrue(message.contains("Data source node"));
        }

        @Test
        void returnsTemplateWhenParamsAreOmitted() {
            String message = MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO");

            assertTrue(message.contains("Node {0} passes the check"));
        }

        @Test
        void formatsChineseTemplateWithDefaultNodeName() {
            String message = MessageUtil.getDagCheckMsg(Locale.CHINA, "SOURCE_SETTING_INFO", "数据源节点");

            assertTrue(message.contains("节点数据源节点检测通过"));
        }
    }

    @Nested
    class LocalizeDagNodeNameTest {
        @Test
        void localizesDatabaseDefaultName() {
            assertEquals("Data source node", MessageUtil.localizeDagNodeName(Locale.US, "数据源节点"));
        }

        @Test
        void localizesJsProcessorDefaultName() {
            assertEquals("Enhanced JS node", MessageUtil.localizeDagNodeName(Locale.US, "增强JS节点"));
        }

        @Test
        void preservesCustomNodeName() {
            assertEquals("我的中文节点", MessageUtil.localizeDagNodeName(Locale.US, "我的中文节点"));
        }

        @Test
        void keepsDefaultNodeNameForChineseLocale() {
            assertEquals("数据源节点", MessageUtil.localizeDagNodeName(Locale.CHINA, "数据源节点"));
        }

        @Test
        void fallsBackToOriginalNameWhenBundleValueIsBlank() {
            assertEquals("表节点", MessageUtil.localizeDagNodeName(Locale.US, "表节点"));
        }

        @Test
        void returnsBlankNodeNameUnchanged() {
            assertEquals("", MessageUtil.localizeDagNodeName(Locale.US, ""));
            assertEquals("   ", MessageUtil.localizeDagNodeName(Locale.US, "   "));
            assertNull(MessageUtil.localizeDagNodeName(Locale.US, null));
        }
    }

    @Nested
    class LocalizeDagCheckParamsTest {
        @Test
        void returnsEmptyArrayInsteadOfNull() {
            Object[] localized = MessageUtil.localizeDagCheckParams(Locale.US, null);

            assertNotNull(localized);
            assertEquals(0, localized.length);
        }

        @Test
        void keepsEmptyArray() {
            Object[] localized = MessageUtil.localizeDagCheckParams(Locale.US, new Object[0]);

            assertNotNull(localized);
            assertEquals(0, localized.length);
        }

        @Test
        void localizesStringsAndLeavesOtherTypes() {
            Date timestamp = new Date();
            Object[] localized = MessageUtil.localizeDagCheckParams(
                    Locale.US,
                    new Object[]{"数据源节点", timestamp, 3, null, "我的自定义节点"});

            assertArrayEquals(new Object[]{"Data source node", timestamp, 3, null, "我的自定义节点"}, localized);
        }
    }
}
