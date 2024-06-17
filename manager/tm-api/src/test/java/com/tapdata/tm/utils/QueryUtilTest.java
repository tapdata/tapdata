package com.tapdata.tm.utils;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

class QueryUtilTest {
    @Nested
    class ParseWhereToCriteriaTest {
        @Test
        void test() {
            Assertions.assertNotNull(QueryUtil.parseWhereToCriteria(new Where()));
        }
    }

    @Nested
    class parsePageParam {
        @Test
        void testNormal() {
            Filter filter = new Filter();
            Query query = new Query();
            Assertions.assertDoesNotThrow(() -> QueryUtil.parsePageParam(filter, query));
        }
    }

    @Nested
    class parseOrder {
        Filter filter;
        @BeforeEach
        void init() {
            filter = new Filter();
            filter.setSort(Lists.newArrayList("add DESC", "edd ASC", "err", "kk cc"));
        }

        @Test
        void testNormal() {
            List<Sort> sorts = QueryUtil.parseOrder(filter);
            Assertions.assertNotNull(sorts);
            Assertions.assertEquals(3, sorts.size());
        }
    }
}