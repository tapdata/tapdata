package com.tapdata.tm.base.dto;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

class PageTest {

    @Nested
    class PageMethodTest {
        @Test
        void testNormal() {
            Page<Object> page = Page.page(new ArrayList<>(), 10L);
            Assertions.assertEquals(10L, page.getTotal());
            Assertions.assertTrue(page.getItems().isEmpty());
        }
        @Test
        void testNull() {
            Page<Object> page = Page.page(null, null);
            Assertions.assertEquals(0L, page.getTotal());
            Assertions.assertTrue(page.getItems().isEmpty());
        }

        @Test
        void testEmpty() {
            Page<Object> page = Page.empty();
            Assertions.assertEquals(0L, page.getTotal());
            Assertions.assertTrue(page.getItems().isEmpty());
        }
    }
}