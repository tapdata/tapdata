package com.tapdata.tm.mcp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 15:06
 */
public class UtilsTest {

    @Test
    public void testToJson() {
        Map<String, Object> data = new HashMap<>();
        data.put("test", "test");
        data.put("id", 123);
        Assertions.assertEquals("{\n" +
                "  \"test\" : \"test\",\n" +
                "  \"id\" : 123\n" +
                "}", Utils.toJson(data));

        Assertions.assertEquals("[\n" +
                "  \"a\",\n" +
                "  \"b\"\n" +
                "]", Utils.toJson(Arrays.asList("a", "b")));

    }
}
