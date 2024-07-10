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

    @Nested
    class ParseOrderTest {
        String sortKey = "createTime";

        @Test
        void testNotParse() {
            String order = null;
            Assertions.assertNull(QueryUtil.parseOrder(order));

            order = "";
            Assertions.assertNull(QueryUtil.parseOrder(order));

            order = " ";
            Assertions.assertNull(QueryUtil.parseOrder(order));
        }

        @Test
        void testNoDirection() {
            String order = String.format("%s", sortKey);
            Sort sort = QueryUtil.parseOrder(order);
            Assertions.assertNotNull(sort);
            Sort.Order orderFor = sort.getOrderFor(sortKey);
            Assertions.assertNotNull(orderFor);
            Assertions.assertEquals(Sort.Direction.DESC, orderFor.getDirection());
        }

        @Test
        void testAsc() {
            String order = String.format("%s  asc ", sortKey);
            Sort sort = QueryUtil.parseOrder(order);
            Assertions.assertNotNull(sort);
            Sort.Order orderFor = sort.getOrderFor(sortKey);
            Assertions.assertNotNull(orderFor);
            Assertions.assertEquals(Sort.Direction.ASC, orderFor.getDirection());
        }

        @Test
        void testDesc() {
            String order = String.format("%s desc", sortKey);
            Sort sort = QueryUtil.parseOrder(order);
            Assertions.assertNotNull(sort);
            Sort.Order orderFor = sort.getOrderFor(sortKey);
            Assertions.assertNotNull(orderFor);
            Assertions.assertEquals(Sort.Direction.DESC, orderFor.getDirection());

        }
    }
}
