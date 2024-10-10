package com.tapdata.constant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.entity.User;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/10 11:43
 */
public class JSONUtilTest {

    @Test
    public void testJson2List() throws IOException {

        JSONUtil.enableFeature(SerializationFeature.INDENT_OUTPUT);
        JSONUtil.disableFeature(SerializationFeature.CLOSE_CLOSEABLE);

        List<Map> result = JSONUtil.json2List("[{\"a\": 1}]" );
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());

        List<String> result1 = JSONUtil.json2List("[\"a\"]", String.class);
        Assertions.assertNotNull(result1);
        Assertions.assertEquals(1, result1.size());
    }

    @Test
    public void testObject2Json() throws JsonProcessingException {
        List<String> list= Arrays.asList("test");
        String json = JSONUtil.obj2Json(list);

        Assertions.assertNotNull(json);
        Assertions.assertEquals(8, json.length());

        json = JSONUtil.obj2JsonPretty(list);
        Assertions.assertEquals("[ \"test\" ]", json);
    }

    @Test
    public void testJson2Map() throws IOException {
        Map<String, Object> result = JSONUtil.json2Map("{\"a\": 1, \"b\": 1}");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testJson2POJO() throws IOException {
        Map<String, Object> result = JSONUtil.json2POJO("{\"a\": 1, \"b\": 1}", Map.class);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());

        result = JSONUtil.json2POJO("{\"a\": 1, \"b\": 1}", Map.class);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());

        List<User> users = JSONUtil.json2POJO("[{\"id\": \"1\"}]", new TypeReference<List<User>>() {
        });
        Assertions.assertNotNull(users);
        Assertions.assertEquals(1, users.size());
        Assertions.assertEquals("1", users.get(0).getId());

        URL url = getClass().getResource("/test.json");
        User user = JSONUtil.json2POJO(url, User.class);
        Assertions.assertNotNull(user);
        Assertions.assertEquals("1", user.getId());

        user = JSONUtil.json2POJO(url, new TypeReference<User>() {
        });
        Assertions.assertNotNull(user);
        Assertions.assertEquals("1", user.getId());
    }

    @Test
    public void testMap2POJO() throws JsonProcessingException {
        Map<String, String> map = new HashMap<>();
        map.put("id", "1");
        User user = JSONUtil.map2POJO(map, User.class);
        Assertions.assertNotNull(user);
        Assertions.assertEquals("1", user.getId());

        user = JSONUtil.map2POJO(map, new TypeReference<User>() {
        });
        Assertions.assertNotNull(user);
        Assertions.assertEquals("1", user.getId());
    }

    @Test
    public void testMap2Json() throws JsonProcessingException {
        Map<String, String> map = new HashMap<>();
        map.put("id", "1");
        String json = JSONUtil.map2Json(map);
        Assertions.assertNotNull(json);
        Assertions.assertEquals("{\"id\":\"1\"}", json);

        json = JSONUtil.map2JsonPretty(map);
        Assertions.assertNotNull(json);
        Assertions.assertEquals("{\n" +
                "  \"id\" : \"1\"\n" +
                "}", json);
    }

    @Test
    public void testTapType() throws IOException {
        List<TapType> types = new ArrayList<>();
        types.add(new TapString(10L, true));
        String result = JSONUtil.obj2Json(types);
        Assertions.assertNotNull(result);

        List<TapType> list = JSONUtil.json2POJO(result, new TypeReference<List<TapType>>() {
        });
        Assertions.assertNotNull(list);
        Assertions.assertEquals(1, list.size());
        Assertions.assertInstanceOf(TapString.class, list.get(0));
    }

}
