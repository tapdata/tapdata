package com.tapdata.tm.system.api.utils;


import com.alibaba.fastjson.JSON;
import com.tapdata.tm.module.dto.Param;
import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import com.tapdata.tm.system.api.enums.OutputType;
import com.tapdata.tm.system.api.vo.DebugVo;
import com.tapdata.tm.utils.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.assertions.Assertions.assertNull;
import static org.bson.assertions.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TextEncryptionUtilTest {

    @Nested
    class textEncryptionBySwitchTest {
        @Test
        void testOpen() {
            String json = "[{\"limit\": 1,\"page\":10, \"code\":\"xxsx\", \"filter\":{\"fields\":[\"name\"],\"where\":{\"name\":\"test\"},\"sort\":{\"name\":1},\"order\":\"name asc\"}}]";
            List<Map<String, Object>> maps = (List<Map<String, Object>>)(List<?>) JSON.parseArray(json);
            List<Map<String, Object>> parseObject = TextEncryptionUtil.textEncryptionBySwitch(true, maps);
            assertNotNull(parseObject);
            assertEquals(maps.size(), parseObject.size());
            assertEquals(maps.get(0).size(), parseObject.get(0).size());
            assertEquals(maps.get(0).get("limit"), parseObject.get(0).get("limit"));
            assertEquals(maps.get(0).get("page"), parseObject.get(0).get("page"));
            assertEquals(((Map<Object, Object>) maps.get(0).get("filter")).get("order"), ((Map<Object, Object>) parseObject.get(0).get("filter")).get("order"));
            assertEquals(((Map<Object, Object>) maps.get(0).get("filter")).get("sort"), ((Map<Object, Object>) parseObject.get(0).get("filter")).get("sort"));
            assertEquals(((Map<Object, Object>) maps.get(0).get("filter")).get("fields"), ((Map<Object, Object>) parseObject.get(0).get("filter")).get("fields"));
            assertEquals(TextEncryptionUtil.PARAM_REPLACE_CHAR, ((Map<Object, Object>) ((Map<Object, Object>) maps.get(0).get("filter")).get("where")).get("name"));
        }
        @Test
        void testNotOpen() {
            String json = "[{\"limit\": 1,\"page\":10, \"filter\":{\"fields\":[\"name\"],\"where\":{\"name\":\"test\"},\"sort\":{\"name\":1},\"order\":\"name asc\"}}]";
            List<Map<String, Object>> maps = (List<Map<String, Object>>)(List<?>) JSON.parseArray(json);
            List<Map<String, Object>> parseObject = TextEncryptionUtil.textEncryptionBySwitch(false, maps);
            assertNotNull(parseObject);
            assertEquals(maps.size(), parseObject.size());
            assertEquals(maps.get(0).size(), parseObject.get(0).size());
            assertEquals(maps.get(0).get("limit"), parseObject.get(0).get("limit"));
            assertEquals(maps.get(0).get("page"), parseObject.get(0).get("page"));
            assertEquals(((Map<Object, Object>) maps.get(0).get("filter")).get("order"), ((Map<Object, Object>) parseObject.get(0).get("filter")).get("order"));
            assertEquals(((Map<Object, Object>) maps.get(0).get("filter")).get("sort"), ((Map<Object, Object>) parseObject.get(0).get("filter")).get("sort"));
            assertEquals(((Map<Object, Object>) maps.get(0).get("filter")).get("fields"), ((Map<Object, Object>) parseObject.get(0).get("filter")).get("fields"));
            assertEquals(((Map<Object, Object>) ((Map<Object, Object>) maps.get(0).get("filter")).get("where")).get("name"), ((Map<Object, Object>) ((Map<Object, Object>) maps.get(0).get("filter")).get("where")).get("name"));
        }
        @Test
        void testFilterIsString() {
            String json = "[{\"limit\": 1,\"page\":10, \"filter\":\"{\\\"fields\\\":[\\\"name\\\"],\\\"where\\\":{\\\"name\\\":\\\"test\\\"},\\\"sort\\\":{\\\"name\\\":1},\\\"order\\\":\\\"name asc\\\"}\"}]";
            List<Map<String, Object>> maps = (List<Map<String, Object>>)(List<?>) JSON.parseArray(json);
            List<Map<String, Object>> parseObject = TextEncryptionUtil.textEncryptionBySwitch(true, maps);
            assertNotNull(parseObject);
            assertEquals(maps.size(), parseObject.size());
            assertEquals(maps.get(0).size(), parseObject.get(0).size());
            assertEquals(maps.get(0).get("limit"), parseObject.get(0).get("limit"));
            assertEquals(maps.get(0).get("page"), parseObject.get(0).get("page"));
            assertEquals(((Map<Object, Object>) maps.get(0).get("filter")).get("order"), ((Map<Object, Object>) parseObject.get(0).get("filter")).get("order"));
            assertEquals(((Map<Object, Object>) maps.get(0).get("filter")).get("sort"), ((Map<Object, Object>) parseObject.get(0).get("filter")).get("sort"));
            assertEquals(((Map<Object, Object>) maps.get(0).get("filter")).get("fields"), ((Map<Object, Object>) parseObject.get(0).get("filter")).get("fields"));
            assertEquals(((Map<Object, Object>) ((Map<Object, Object>) maps.get(0).get("filter")).get("where")).get("name"), ((Map<Object, Object>) ((Map<Object, Object>) maps.get(0).get("filter")).get("where")).get("name"));
        }
        @Test
        void testEmpty() {
            String json = "[]";
            List<Map<String, Object>> maps = (List<Map<String, Object>>)(List<?>) JSON.parseArray(json);
            List<Map<String, Object>> parseObject = TextEncryptionUtil.textEncryptionBySwitch(true, maps);
            assertNotNull(parseObject);
            assertEquals(maps.size(), parseObject.size());
        }
        @Test
        void testFilterNotJson() {
            String json = "[{\"filter\": \"xxsdf\"}]";
            List<Map<String, Object>> maps = (List<Map<String, Object>>)(List<?>) JSON.parseArray(json);
            List<Map<String, Object>> parseObject = TextEncryptionUtil.textEncryptionBySwitch(true, maps);
            assertNotNull(parseObject);
            assertEquals(maps.size(), parseObject.size());
        }
        @Test
        void testCharter() {
            List<Map<String, Object>> maps = new ArrayList<>();
            Map<String, Object> map = new HashMap<>();
            map.put("name", (char) 'c');
            maps.add(map);
            List<Map<String, Object>> parseObject = TextEncryptionUtil.textEncryptionBySwitch(true, maps);
            assertNotNull(parseObject);
            assertEquals(maps.size(), parseObject.size());
        }
        @Test
        void testArray() {
            List<Map<String, Object>> maps = new ArrayList<>();
            Map<String, Object> map = new HashMap<>();
            maps.add(map);
            Map<String, Object> sub = new HashMap<>();
            sub.put("name", "test");
            sub.put("id", null);
            sub.put("ch", (char) 'g');
            sub.put("coll", Lists.newArrayList("xxs", "nb"));
            sub.put("date", new Date());
            map.put("arr", Lists.newArrayList(sub, Lists.newArrayList("xxs", "nb")));
            List<Map<String, Object>> parseObject = TextEncryptionUtil.textEncryptionBySwitch(true, maps);
            assertNotNull(parseObject);
            assertEquals(maps.size(), parseObject.size());
        }
        @Test
        void testOther() {
            List<Map<String, Object>> maps = new ArrayList<>();
            Map<String, Object> map = new HashMap<>();
            map.put("other", new Date());
            maps.add(map);
            List<Map<String, Object>> parseObject = TextEncryptionUtil.textEncryptionBySwitch(true, maps);
            assertNotNull(parseObject);
            assertEquals(maps.size(), parseObject.size());
        }
        @Test
        void testNull() {
            List<Map<String, Object>> parseObject = TextEncryptionUtil.textEncryptionBySwitch(true, null);
            assertNull(parseObject);
        }
    }

    @Nested
    class mapTest {

        @Test
        void testNormal() {
            TextEncryptionRuleDto rule = new TextEncryptionRuleDto();
            rule.setName("name");
            rule.setRegex(".*");
            rule.setOutputChar("*");
            rule.setType(0);
            rule.setOutputType(OutputType.AUTO.getCode());
            Map<String, List<TextEncryptionRuleDto>> config = new HashMap<>();
            config.put("name", Lists.newArrayList(rule));
            config.put("address", Lists.newArrayList(rule));
            config.put("nullK", Lists.newArrayList(rule));
            config.put("other.id", Lists.newArrayList(rule));
            config.put("other.kard", Lists.newArrayList(rule));
            config.put("other.info", Lists.newArrayList(rule));
            config.put("other.numbers", Lists.newArrayList(rule));
            config.put("other.numB", Lists.newArrayList(rule));
            config.put("other.numS", Lists.newArrayList(rule));
            config.put("other.numI", Lists.newArrayList(rule));
            config.put("other.numL", Lists.newArrayList(rule));
            config.put("other.numD", Lists.newArrayList(rule));
            config.put("other.numBL", Lists.newArrayList(rule));
            config.put("other.numBD", Lists.newArrayList(rule));
            config.put("other.date", Lists.newArrayList(rule));
            config.put("other.chr", Lists.newArrayList(rule));
            config.put("other.nullK", Lists.newArrayList(rule));
            config.put("other.array", Lists.newArrayList(rule));
            config.put("other.bool", Lists.newArrayList(rule));
            config.put("other.customObj", Lists.newArrayList(rule));
            DebugVo debugVo = new DebugVo();
            List<Map<String, Object>> data = new ArrayList<>();
            debugVo.setData(data);
            data.add(new HashMap<>());
            data.get(0).put("limit", 1);
            data.get(0).put("page", 1);
            data.get(0).put("name", "gavin'xiao");
            data.get(0).put("address", Lists.newArrayList("shenzhen", "beijing", "ji'an", "guangzhou", "nancang"));
            Map<String, Object> infoMap = new HashMap<>();
            infoMap.put("id", "xxssxxojd");
            infoMap.put("kard", "ggodu");
            infoMap.put("numB", (byte) 1);
            infoMap.put("numS", (short) 1);
            infoMap.put("numI", 10);
            infoMap.put("numL", 10L);
            infoMap.put("numF", 10.9f);
            infoMap.put("numD", 10.01d);
            infoMap.put("numBL", new BigInteger("998"));
            infoMap.put("numBD", new BigDecimal("12.98"));
            infoMap.put("date", new Date());
            infoMap.put("chr", 'q');
            infoMap.put("nullK", null);
            infoMap.put("array", new String[]{"k1", "k2", "k3"});
            infoMap.put("bool", true);
            infoMap.put("customObj", new Object());
            infoMap.put("numbers", Lists.newArrayList("you", "are", "somebody"));
            infoMap.put("info", Map.of("key", "value"));
            data.get(0).put("other", infoMap);
            data.get(0).put("nullK", null);
            System.out.println(JSON.toJSONString(debugVo.getData()));
            TextEncryptionUtil.map(config, debugVo);
            System.out.println(JSON.toJSONString(debugVo.getData()));
            Assertions.assertEquals(
                    JSON.toJSONString(debugVo.getData()),
                    "[{\"other\":{\"date\":\"**\",\"bool\":\"**\",\"customObj\":{},\"numbers\":[\"**\",\"**\",\"**\"],\"numBD\":\"**\",\"chr\":\"**\",\"numD\":\"**\",\"numB\":\"**\",\"numI\":\"**\",\"numF\":10.9,\"numBL\":\"**\",\"numL\":\"**\",\"array\":[\"**\",\"**\",\"**\"],\"kard\":\"**\",\"id\":\"**\",\"numS\":\"**\",\"info\":{\"key\":\"**\"}},\"address\":[\"**\",\"**\",\"**\",\"**\",\"**\"],\"limit\":1,\"name\":\"**\",\"page\":1}]"
            );
        }

        @Test
        void testEmptyConfig() {
            DebugVo debugVo = new DebugVo();
            DebugVo map = TextEncryptionUtil.map(new HashMap<>(), debugVo);
            Assertions.assertEquals(map.getData(), debugVo.getData());
        }
        @Test
        void testNullConfig() {
            DebugVo debugVo = new DebugVo();
            DebugVo map = TextEncryptionUtil.map(null, debugVo);
            Assertions.assertEquals(map.getData(), debugVo.getData());
        }
        @Test
        void testNullDebugVo() {
            DebugVo map = TextEncryptionUtil.map(new HashMap<>(), (DebugVo) null);
            Assertions.assertNull(map);
        }
        @Test
        void testNullDebugData() {
            DebugVo debugVo = new DebugVo();
            DebugVo map = TextEncryptionUtil.map(new HashMap<>(), debugVo);
            Assertions.assertNull(map.getData());
        }
        @Test
        void testEmptyDebugData() {
            DebugVo debugVo = new DebugVo();
            debugVo.setData(new ArrayList<>());
            DebugVo map = TextEncryptionUtil.map(new HashMap<>(), debugVo);
            Assertions.assertNotNull(map.getData());
        }
    }

    @Nested
    class formatFilterTest {
        @Test
        void testFilterNotString() {
            Map<String, Object> map = new HashMap<>();
            map.put(TextEncryptionUtil.FILTER, 1);
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("object");
            paramTypeMap.put(TextEncryptionUtil.FILTER, p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get(TextEncryptionUtil.FILTER), 1);
        }
        @Test
        void testFilterIsBlankString() {
            Map<String, Object> map = new HashMap<>();
            map.put(TextEncryptionUtil.FILTER, "");
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("object");
            paramTypeMap.put(TextEncryptionUtil.FILTER, p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertNotNull(map.get(TextEncryptionUtil.FILTER));
            Assertions.assertEquals(map.get(TextEncryptionUtil.FILTER).getClass(), HashMap.class);
            Assertions.assertEquals(((Map<String, Object>) map.get(TextEncryptionUtil.FILTER)).size(), 0);
        }
        @Test
        void testFilterNotJsonString() {
            Map<String, Object> map = new HashMap<>();
            map.put(TextEncryptionUtil.FILTER, "xxx");
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("object");
            paramTypeMap.put(TextEncryptionUtil.FILTER, p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get("filter"), "xxx");
        }
        @Test
        void testFilterIsJsonString() {
            Map<String, Object> map = new HashMap<>();
            map.put(TextEncryptionUtil.FILTER, "{\"id\":\"id\"}");
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("object");
            paramTypeMap.put(TextEncryptionUtil.FILTER, p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertNotNull(map.get("filter"));
            Assertions.assertEquals(JSON.toJSONString(map.get("filter")), "{\"id\":\"id\"}");
        }
        @Test
        void testPageIsNumberString() {
            Map<String, Object> map = new HashMap<>();
            map.put(TextEncryptionUtil.PAGE, "1");
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("number");
            paramTypeMap.put(TextEncryptionUtil.PAGE, p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get(TextEncryptionUtil.PAGE), 1);
        }
        @Test
        void testPageIsNumber() {
            Map<String, Object> map = new HashMap<>();
            map.put(TextEncryptionUtil.PAGE, 1);
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("number");
            paramTypeMap.put(TextEncryptionUtil.PAGE, p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get(TextEncryptionUtil.PAGE), 1);
        }
        @Test
        void testPageIsNotNumber() {
            Map<String, Object> map = new HashMap<>();
            map.put(TextEncryptionUtil.PAGE, "xxx");
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("number");
            paramTypeMap.put(TextEncryptionUtil.PAGE, p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get(TextEncryptionUtil.PAGE), "xxx");
        }
        @Test
        void testLimitIsNumberString() {
            Map<String, Object> map = new HashMap<>();
            map.put(TextEncryptionUtil.LIMIT, "1");
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("number");
            paramTypeMap.put(TextEncryptionUtil.LIMIT, p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get(TextEncryptionUtil.LIMIT), 1);
        }
        @Test
        void testLimitIsNumber() {
            Map<String, Object> map = new HashMap<>();
            map.put(TextEncryptionUtil.LIMIT, 1);
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("number");
            paramTypeMap.put(TextEncryptionUtil.LIMIT, p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get(TextEncryptionUtil.LIMIT), 1);
        }
        @Test
        void testLimitIsNotNumber() {
            Map<String, Object> map = new HashMap<>();
            map.put(TextEncryptionUtil.LIMIT, "xxx");
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("number");
            paramTypeMap.put(TextEncryptionUtil.LIMIT, p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get(TextEncryptionUtil.LIMIT), "xxx");
        }

        @Test
        void testBooleanIsBooleanString() {
            Map<String, Object> map = new HashMap<>();
            map.put("bool", "true");
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("boolean");
            paramTypeMap.put("bool", p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get("bool"), true);
        }

        @Test
        void testBooleanIsBoolean() {
            Map<String, Object> map = new HashMap<>();
            map.put("bool", true);
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("boolean");
            paramTypeMap.put("bool", p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get("bool"), true);
        }
        @Test
        void testBooleanIsNotBoolean() {
            Map<String, Object> map = new HashMap<>();
            map.put("bool", "xxx");
            Map<String, Param> paramTypeMap = new HashMap<>();
            Param p = new Param();
            p.setType("boolean");
            paramTypeMap.put("bool", p);
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get("bool"), "xxx");
        }
        @Test
        void testParamTypeMapIsEmpty() {
            Map<String, Object> map = new HashMap<>();
            map.put("bool", "1");
            Map<String, Param> paramTypeMap = new HashMap<>();
            TextEncryptionUtil.formatBefore(map, paramTypeMap);
            Assertions.assertEquals(map.get("bool"), "1");
        }
        @Test
        void testParamTypeMapIsNull() {
            Map<String, Object> map = new HashMap<>();
            map.put("bool", "1");
            TextEncryptionUtil.formatBefore(map, null);
            Assertions.assertEquals(map.get("bool"), "1");
        }
    }
}