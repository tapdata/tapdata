package com.tapdata.tm.commons.dag;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AccessNodeTypeEnumTest {
    @Test
    void testParams() {
        Assertions.assertEquals("AUTOMATIC_PLATFORM_ALLOCATION", AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
        Assertions.assertEquals("MANUALLY_SPECIFIED_BY_THE_USER", AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        Assertions.assertEquals("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP", AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        Assertions.assertEquals("平台自动分配", AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.getName());
        Assertions.assertEquals("用户手动指定", AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.getName());
        Assertions.assertEquals("用户手动指定-引擎组", AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.getName());
    }

    @Nested
    class IsManuallyTest {
        @Test
        void testIsUserManually() {
            Assertions.assertTrue(AccessNodeTypeEnum.isManually("MANUALLY_SPECIFIED_BY_THE_USER"));
        }

        @Test
        void testNotUserManuallyButIsGroupManually() {
            Assertions.assertTrue(AccessNodeTypeEnum.isManually("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP"));
        }

        @Test
        void testNotUserManuallyAndNotGroupManually() {
            Assertions.assertFalse(AccessNodeTypeEnum.isManually("AUTOMATIC_PLATFORM_ALLOCATION"));
        }
    }

    @Nested
    class IsUserManuallyTest {
        @Test
        void testIsIsUserManually() {
            Assertions.assertTrue(AccessNodeTypeEnum.isUserManually("MANUALLY_SPECIFIED_BY_THE_USER"));
        }
        @Test
        void testNotIsUserManually() {
            Assertions.assertFalse(AccessNodeTypeEnum.isUserManually("MANUALLY_SPECIFIED_BY_THE_USER1"));
        }
    }
    @Nested
    class IsGroupManuallyTest {
        @Test
        void testIsIsGroupManually() {
            Assertions.assertTrue(AccessNodeTypeEnum.isGroupManually("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP"));
        }
        @Test
        void testNotIsGroupManually() {
            Assertions.assertFalse(AccessNodeTypeEnum.isGroupManually("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUpp"));
        }
    }
}