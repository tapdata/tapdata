package com.tapdata.tm.utils;

import com.tapdata.tm.base.dto.Where;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class QueryUtilTest {
    @Nested
    class ParseWhereToCriteriaTest {
        @Test
        void test() {
            Assertions.assertNotNull(QueryUtil.parseWhereToCriteria(new Where()));
        }
    }
}