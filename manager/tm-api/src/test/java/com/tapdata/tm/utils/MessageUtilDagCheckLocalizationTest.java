package com.tapdata.tm.utils;

import org.junit.jupiter.api.Nested;
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

    @Nested
    class GetDagCheckMsgTest {
        @Test
        void localizesDefaultNodeNameInEnglishMessage() {
            String message = MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO",
                    MessageUtil.dagNodeName("database", "数据源节点"));

            assertTrue(message.contains("Data source node"));
        }

        @Test
        void returnsTemplateWhenParamsAreOmitted() {
            String message = MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO");

            assertTrue(message.contains("Node {0} passes the check"));
        }

        @Test
        void formatsChineseTemplateWithDefaultNodeName() {
            String message = MessageUtil.getDagCheckMsg(Locale.CHINA, "SOURCE_SETTING_INFO",
                    MessageUtil.dagNodeName("database", "数据源节点"));

            assertTrue(message.contains("节点数据源节点检测通过"));
        }

        @Test
        void doesNotRewritePlainStringMatchingADefaultNodeName() {
            String message = MessageUtil.getDagCheckMsg(Locale.US, "SOURCE_SETTING_INFO", "数据源节点");

            assertTrue(message.contains("数据源节点"));
            assertFalse(message.contains("Data source node"));
        }

        @Test
        void returnsMissingKeyInsteadOfJoiningParams() {
            assertEquals("MISSING_DAG_CHECK_KEY",
                    MessageUtil.getDagCheckMsg(Locale.CHINA, "MISSING_DAG_CHECK_KEY", (Object) null));
        }
    }

    @Nested
    class LocalizeDagNodeNameTest {
        @Test
        void localizesDatabaseDefaultName() {
            assertEquals("Data source node", MessageUtil.localizeDagNodeName(Locale.US, "database", "数据源节点"));
        }

        @Test
        void localizesJsProcessorDefaultName() {
            assertEquals("Enhanced JS node", MessageUtil.localizeDagNodeName(Locale.US, "js_processor", "增强JS节点"));
        }

        @Test
        void preservesCustomNodeName() {
            assertEquals("我的中文节点", MessageUtil.localizeDagNodeName(Locale.US, "database", "我的中文节点"));
        }

        @Test
        void preservesDefaultNameOfADifferentNodeType() {
            assertEquals("数据源节点", MessageUtil.localizeDagNodeName(Locale.US, "table", "数据源节点"));
        }

        @Test
        void keepsDefaultNodeNameForChineseLocale() {
            assertEquals("数据源节点", MessageUtil.localizeDagNodeName(Locale.CHINA, "database", "数据源节点"));
        }

        @Test
        void fallsBackToOriginalNameWhenBundleValueIsBlank() {
            assertEquals("表节点", MessageUtil.localizeDagNodeName(Locale.US, "unknown", "表节点"));
        }

        @Test
        void returnsBlankNodeNameUnchanged() {
            assertEquals("", MessageUtil.localizeDagNodeName(Locale.US, "database", ""));
            assertEquals("   ", MessageUtil.localizeDagNodeName(Locale.US, "database", "   "));
            assertNull(MessageUtil.localizeDagNodeName(Locale.US, "database", null));
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
        void localizesOnlyTypedNodeNames() {
            Date timestamp = new Date();
            Object[] localized = MessageUtil.localizeDagCheckParams(
                    Locale.US,
                    new Object[]{MessageUtil.dagNodeName("database", "数据源节点"), "数据源节点", timestamp, 3, null, "我的自定义节点"});

            assertArrayEquals(new Object[]{"Data source node", "数据源节点", timestamp, 3, null, "我的自定义节点"}, localized);
        }
    }

    @Nested
    class GetStringNullParamTest {
        @Test
        void missingMessageKeyWithNullParamDoesNotThrow() {
            String message = assertDoesNotThrow(
                    () -> MessageUtil.getMessage(Locale.US, "THIS_KEY_SHOULD_NOT_EXIST", (Object) null));
            assertEquals("null", message);
        }
    }
}
