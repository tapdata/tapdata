package com.tapdata.tm.utils;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.base.exception.BizException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Document;

class MongoUtilsTest {
    @Nested
    class getCollectionNameTest {
        @Document("MockEntity")
        class MockEntity extends BaseEntity {
            private String name;
            private String age;
        }

        class Mock2Entity extends BaseEntity {
            private String name;
            private String age;
        }

        @Test
        void testNormal() {
            Assertions.assertEquals("MockEntity", MongoUtils.getCollectionName(MockEntity.class));
        }

        @Test
        void testException() {
            Assertions.assertThrows(BizException.class, () -> MongoUtils.getCollectionName(Mock2Entity.class));
        }
    }
}