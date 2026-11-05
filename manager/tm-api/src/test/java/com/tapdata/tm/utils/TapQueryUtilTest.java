package com.tapdata.tm.utils;

import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class TapQueryUtilTest {

    @Test
    @DisplayName("test private constructor")
    void testPrivateConstructor() throws Exception {
        Constructor<TapQueryUtil> constructor = TapQueryUtil.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        TapQueryUtil instance = constructor.newInstance();
        Assertions.assertNotNull(instance);
    }

    @Nested
    class BuildQueryTest {

        @Test
        @DisplayName("test build query with empty filter")
        void testEmptyFilter() {
            Filter filter = new Filter();
            Query query = TapQueryUtil.buildQuery(filter, Settings.class);
            Assertions.assertNotNull(query);
            Document queryObj = query.getQueryObject();
            Assertions.assertEquals(new Document("$ne", true), queryObj.get("is_deleted"));
            
            Document sortObj = query.getSortObject();
            Assertions.assertEquals(new Document("createAt", -1), sortObj);
        }

        @Test
        @DisplayName("test build query with null where")
        void testNullWhere() {
            Filter filter = new Filter();
            filter.setWhere(null);
            Query query = TapQueryUtil.buildQuery(filter, Settings.class);
            
            Document queryObj = query.getQueryObject();
            Assertions.assertEquals(new Document("$ne", true), queryObj.get("is_deleted"));
            Assertions.assertNull(queryObj.get("category"));
        }

        @Test
        @DisplayName("test build query with simple where")
        void testSimpleWhere() {
            Filter filter = new Filter();
            filter.setWhere(new Where().and("category", "testCategory"));
            Query query = TapQueryUtil.buildQuery(filter, Settings.class);
            
            Document queryObj = query.getQueryObject();
            Assertions.assertEquals("testCategory", queryObj.get("category"));
            Assertions.assertEquals(new Document("$ne", true), queryObj.get("is_deleted"));
        }

        @Test
        @DisplayName("test build query with $in where")
        void testInWhere() {
            Filter filter = new Filter();
            Map<String, Object> inMap = new HashMap<>();
            inMap.put("$in", Arrays.asList("a", "b"));
            filter.setWhere(new Where().and("key", inMap));
            
            Query query = TapQueryUtil.buildQuery(filter, Settings.class);
            Document queryObj = query.getQueryObject();
            
            Document keyDoc = queryObj.get("key", Document.class);
            Assertions.assertNotNull(keyDoc);
            Assertions.assertEquals(Arrays.asList("a", "b"), keyDoc.get("$in"));
        }

        @Test
        @DisplayName("test build query with order ASC")
        void testDefault() {
            Filter filter = new Filter();
            filter.setOrder(Lists.of("xxx ASC"));
            
            Query query = TapQueryUtil.buildQuery(filter, Settings.class);
            Document sortObj = query.getSortObject();
            Assertions.assertEquals(new Document("createAt", -1), sortObj);
        }

        @Test
        @DisplayName("test build query with order ASC")
        void testOrderAsc() {
            Filter filter = new Filter();
            filter.setOrder(Lists.of("key ASC"));

            Query query = TapQueryUtil.buildQuery(filter, Settings.class);
            Document sortObj = query.getSortObject();
            Assertions.assertEquals(new Document("key", 1), sortObj);
        }

        @Test
        @DisplayName("test build query with order DESC")
        void testOrderDesc() {
            Filter filter = new Filter();
            filter.setOrder(Lists.of("key DESC"));
            
            Query query = TapQueryUtil.buildQuery(filter, Settings.class);
            Document sortObj = query.getSortObject();
            Assertions.assertEquals(new Document("key", -1), sortObj);
        }

        @Test
        @DisplayName("test build query with order DESC")
        void testOrderDesc1() {
            Filter filter = new Filter();
            filter.setOrder(Lists.of("key DESC", "category DESC"));

            Query query = TapQueryUtil.buildQuery(filter, Settings.class);
            Document sortObj = query.getSortObject();
            Assertions.assertTrue(sortObj.containsKey("key"));
            Assertions.assertTrue(sortObj.containsKey("category"));
            Assertions.assertEquals(-1, sortObj.get("category"));
            Assertions.assertEquals(-1, sortObj.get("key"));
        }
    }
}
