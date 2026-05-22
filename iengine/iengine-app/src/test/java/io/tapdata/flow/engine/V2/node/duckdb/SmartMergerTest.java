package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SmartMergerTest {

    @Test
    void testEmptyInput() {
        List<Map<String,Object>> merged = SmartMerger.mergeLastWins(Collections.emptyList());
        assertNotNull(merged);
        assertTrue(merged.isEmpty());
    }

    @Test
    void testLastWinsWithPk() {
        Map<String,Object> a = new HashMap<>();
        a.put("id", 1);
        a.put("name", "first");

        Map<String,Object> b = new HashMap<>();
        b.put("id", 1);
        b.put("name", "second");

        List<Map<String,Object>> in = Arrays.asList(a, b);
        List<Map<String,Object>> out = SmartMerger.mergeLastWins(in);
        assertEquals(1, out.size());
        assertEquals("second", out.get(0).get("name"));
    }

    @Test
    void testFullRowDedupe() {
        Map<String,Object> a = new HashMap<>();
        a.put("name", "x");

        Map<String,Object> b = new HashMap<>();
        b.put("name", "x");

        List<Map<String,Object>> in = Arrays.asList(a, b);
        List<Map<String,Object>> out = SmartMerger.mergeLastWins(in);
        // if serialized equal, last-wins should collapse duplicates -> size 1
        assertEquals(1, out.size());
    }

    @Test
    void testMixedKeys() {
        Map<String,Object> a = new HashMap<>();
        a.put("id", 2);
        a.put("val", "a");

        Map<String,Object> b = new HashMap<>();
        b.put("val", "a");

        Map<String,Object> c = new HashMap<>();
        c.put("id", 2);
        c.put("val", "c");

        List<Map<String,Object>> in = Arrays.asList(a, b, c);
        List<Map<String,Object>> out = SmartMerger.mergeLastWins(in);
        // key for id=2 should be last (c), full-row for b is distinct
        assertEquals(2, out.size());
    }
}
