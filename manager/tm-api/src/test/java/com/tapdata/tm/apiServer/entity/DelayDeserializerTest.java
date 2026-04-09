package com.tapdata.tm.apiServer.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DelayDeserializerTest {
    static class Wrapper {
        @JsonDeserialize(using = DelayDeserializer.class)
        public List<Map<String, Number>> delayList;
    }

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
    }

    @Test
    void deserialize_arrayOfObjects() throws Exception {
        String json = """
        {
          "delayList": [
            { "0": 12, "50": 34.5 },
            { "100": 1 }
          ]
        }
        """;

        Wrapper wrapper = mapper.readValue(json, Wrapper.class);

        assertNotNull(wrapper.delayList);
        assertEquals(2, wrapper.delayList.size());

        Map<String, Number> first = wrapper.delayList.get(0);
        assertEquals(12, first.get("0").intValue());
        assertEquals(34.5d, first.get("50").doubleValue(), 0.000001d);

        Map<String, Number> second = wrapper.delayList.get(1);
        assertEquals(1, second.get("100").intValue());
    }

    @Test
    void deserialize_singleObject() throws Exception {
        String json = """
        {
          "delayList": { "0": 1, "1": 2 }
        }
        """;

        Wrapper wrapper = mapper.readValue(json, Wrapper.class);

        assertNotNull(wrapper.delayList);
        assertEquals(1, wrapper.delayList.size());
        assertEquals(1, wrapper.delayList.get(0).get("0").intValue());
        assertEquals(2, wrapper.delayList.get(0).get("1").intValue());
    }

    @Test
    void deserialize_arrayWithNumberItem() throws Exception {
        String json = """
        {
          "delayList": [123]
        }
        """;

        Wrapper wrapper = mapper.readValue(json, Wrapper.class);

        assertNotNull(wrapper.delayList);
        assertEquals(1, wrapper.delayList.size());
        assertEquals(1, wrapper.delayList.get(0).size());
        assertEquals(1, wrapper.delayList.get(0).get("123").intValue());
    }

    @Test
    void deserialize_objectWithNonNumberValue_skipsInvalidEntry() throws Exception {
        String json = """
        {
          "delayList": { "a": "b", "c": 3 }
        }
        """;

        Wrapper wrapper = mapper.readValue(json, Wrapper.class);

        assertNotNull(wrapper.delayList);
        assertEquals(1, wrapper.delayList.size());
        assertEquals(1, wrapper.delayList.get(0).size());
        assertEquals(3, wrapper.delayList.get(0).get("c").intValue());
        assertFalse(wrapper.delayList.get(0).containsKey("a"));
    }

    @Test
    void deserialize_string_returnsEmptyList() throws Exception {
        String json = """
        {
          "delayList": "not-an-object-or-array"
        }
        """;

        Wrapper wrapper = mapper.readValue(json, Wrapper.class);

        assertNotNull(wrapper.delayList);
        assertTrue(wrapper.delayList.isEmpty());
    }

    @Test
    void deserialize_null_returnsNull() throws Exception {
        String json = """
        {
          "delayList": null
        }
        """;

        Wrapper wrapper = mapper.readValue(json, Wrapper.class);

        assertNull(wrapper.delayList);
    }
}

