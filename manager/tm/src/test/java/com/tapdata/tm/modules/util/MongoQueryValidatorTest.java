package com.tapdata.tm.modules.util;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.exception.BizException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoQueryValidatorTest {


    @Nested
    class CheckWhereTest {

        @Test
        void testNormal() {
            String json = "{\n" +
                    "  \"$and\":[\n" +
                    "    {\n" +
                    "      \"$or\":[\n" +
                    "        {\"name\": {\"$eq\": \"{{name}}\"}},\n" +
                    "        {\"apiVersion\": {\"$eq\": \"{{version}}\"}},\n" +
                    "        {\"status\": {\"$gt\": \"{{status}}\"}},\n" +
                    "        {\"status\": {\"$gte\": \"{{status}}\"}},\n" +
                    "        {\"status\": {\"$in\": \"{{status}}\"}},\n" +
                    "        {\"status\": {\"$lt\": \"{{status}}\"}},\n" +
                    "        {\"status\": {\"$lte\": \"{{status}}\"}},\n" +
                    "        {\"status\": {\"$ne\": \"{{status}}\"}},\n" +
                    "        {\"status\": {\"$nin\": \"{{status}}\"}}\n" +
                    "      ]\n" +
                    "    },{\n" +
                    "      \"$nor\": [\n" +
                    "        {\"id\": {\"$exists\": true}}\n" +
                    "      ]\n" +
                    "    },\n" +
                    "      {\"name\": {\"$type\": \"1\"}},\n" +
                    "      {\"name\": {\"$regex\":\".*\"}},\n" +
                    "      {\"name\": {\"$text\": {\n" +
                    "        \"$search\": \"oid\"\n" +
                    "      }}},\n" +
                    "      {\"name\": {\"$where\": \"{}\"}},\n" +
                    "      {\"name\": {\"$all\": [\"1\", \"2\", \"3\"]}},\n" +
                    "      {\"name\": {\"$elemMatch\": {\n" +
                    "        \"id\": \"{{id}}\"\n" +
                    "      }}},\n" +
                    "      {\"name\":{\"$size\": 10}},\n" +
                    "      {\"name\":{\"$mod\": [1,2]}}\n" +
                    "  ]\n" +
                    "}";
            MongoQueryValidator.ValidationContext context = new MongoQueryValidator.ValidationContext();
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.checkWhere(parse(json), context));
        }

        @Test
        void unSupportOperator() {
            String json = "{\"$and\": [{\"name\": {\"$kid\": \"1\"}}]}";
            MongoQueryValidator.ValidationContext context = new MongoQueryValidator.ValidationContext();
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.checkWhere(parse(json), context);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "module.save.check.where.notAllowedOperator");
                    throw e;
                }
            });
        }

        @Test
        void tooDeep() {
            String json = "{\"$and\": [{\"$or\": [{\"and\": [{\"name\": {\"$like\": \"1\"}}]}]}]}";
            MongoQueryValidator.ValidationContext context = new MongoQueryValidator.ValidationContext(1);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.checkWhere(parse(json), context);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "module.save.check.where.tooDeep");
                    throw e;
                }
            });
        }

        @Test
        void notStart$() {
            String json = "{\"$and\": [{\"name\": {\"like\": \"1\"}}]}";
            MongoQueryValidator.ValidationContext context = new MongoQueryValidator.ValidationContext();
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.checkWhere(parse(json), context);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "module.save.check.where.notOperator");
                    throw e;
                }
            });
        }

        @Test
        void notObject() {
            String json = "null";
            MongoQueryValidator.ValidationContext context = new MongoQueryValidator.ValidationContext(1);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.checkWhere(parse(json), context);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "module.save.check.where.notJson");
                    throw e;
                }
            });
        }
        @Test
        void conditionsNotArray() {
            String json = "{\"$and\": \"and\"}";
            MongoQueryValidator.ValidationContext context = new MongoQueryValidator.ValidationContext(1);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.checkWhere(parse(json), context);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "module.save.check.where.notArray");
                    throw e;
                }
            });
        }
        @Test
        void conditionsIsEmpty() {
            String json = "{\"$and\": []}";
            MongoQueryValidator.ValidationContext context = new MongoQueryValidator.ValidationContext(1);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.checkWhere(parse(json), context);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "module.save.check.where.isEmpty");
                    throw e;
                }
            });
        }

    }

    @Nested
    class IsCustomParamTest {
        @Test
        void testNullValue() {
            Assertions.assertEquals("error", MongoQueryValidator.ValidationResult.failure("error").getError());
            Assertions.assertFalse(MongoQueryValidator.isCustomParam(null));
        }

        @Test
        void testNotTextual() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(false);
            Assertions.assertFalse(MongoQueryValidator.isCustomParam(value));
        }
    }

    @Nested
    class GtOrGteOrLtOrLteTest {
        @Test
        void testNotNumber() {
            JsonNode value = mock(JsonNode.class);
            when(value.isNumber()).thenReturn(false);
            when(value.isTextual()).thenReturn(false);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.gtOrGteOrLtOrLte("",value);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "module.save.check.where.notNumberOrDate");
                    throw e;
                }
            });        }

        @Test
        void testNotTextual() {
            JsonNode value = mock(JsonNode.class);
            when(value.isNumber()).thenReturn(true);
            when(value.isTextual()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.gtOrGteOrLtOrLte("", value));
        }

        @Test
        void testNormal() {
            JsonNode value = mock(JsonNode.class);
            when(value.isNumber()).thenReturn(true);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("value");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.gtOrGteOrLtOrLte("", value));
        }

        @Test
        void testIsCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isNumber()).thenReturn(true);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("{{value}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.gtOrGteOrLtOrLte("", value));
        }
    }

    @Nested
    class InOrNinOrAllTest {

        @Test
        void testNormal() {
            JsonNode value = mock(JsonNode.class);
            when(value.isArray()).thenReturn(false);
            when(value.isTextual()).thenReturn(false);
            when(value.textValue()).thenReturn("{{value}}");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.inOrNinOrAll("", value);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "module.save.check.where.notArray");
                    throw e;
                }
            });
        }

        @Test
        void testIsCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isArray()).thenReturn(false);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("{{value}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.inOrNinOrAll("", value));
        }

        @Test
        void testIsArray() {
            JsonNode value = mock(JsonNode.class);
            when(value.isArray()).thenReturn(true);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("{{value}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.inOrNinOrAll("", value));
        }

        @Test
        void testNotIsTextual() {
            JsonNode value = mock(JsonNode.class);
            when(value.isArray()).thenReturn(true);
            when(value.isTextual()).thenReturn(false);
            when(value.textValue()).thenReturn("{{value}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.inOrNinOrAll("", value));
        }
    }

    @Nested
    class RegexTest {
        @Test
        void testNormal() {
            JsonNode value = mock(JsonNode.class);
            when(value.isArray()).thenReturn(false);
            when(value.isTextual()).thenReturn(false);
            when(value.textValue()).thenReturn("{{value}}");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.regex("", value);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "module.save.check.where.notString");
                    throw e;
                }
            });
        }
        @Test
        void testNotPatten() {
            JsonNode value = mock(JsonNode.class);
            when(value.isArray()).thenReturn(false);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("");
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.regex("", value);
                } catch (BizException e) {
                    Assertions.assertEquals(e.getErrorCode(), "module.save.check.where.notRegex");
                    throw e;
                }
            });
        }
        @Test
        void testIsCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("{{name}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.regex("", value));
        }
    }

    @Nested
    class OptionsTest {
        @Test
        void testNormal() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(false);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.options("", value);
                } catch (BizException e) {
                    Assertions.assertEquals("module.save.check.where.notString", e.getErrorCode());
                    throw e;
                }
            });
        }
        @Test
        void testIsCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("{{name}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.options("", value));
        }
        @Test
        void testIsNotCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("name");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.options("", value));
        }
    }

    @Nested
    class ExistsTest {
        @Test
        void testNormal() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(false);
            when(value.isBoolean()).thenReturn(false);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.exists("", value);
                } catch (BizException e) {
                    Assertions.assertEquals("module.save.check.where.notBoolean", e.getErrorCode());
                    throw e;
                }
            });
        }
        @Test
        void testIsCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.isBoolean()).thenReturn(true);
            when(value.textValue()).thenReturn("{{name}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.exists("", value));
        }
        @Test
        void testIsNotCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.isBoolean()).thenReturn(true);
            when(value.textValue()).thenReturn("name");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.exists("", value));
        }
    }


    @Nested
    class WhereTest {
        @Test
        void testNormal() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(false);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.where(value);
                } catch (BizException e) {
                    Assertions.assertEquals("module.save.check.where.notWhereString", e.getErrorCode());
                    throw e;
                }
            });
        }
        @Test
        void testIsCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("{{name}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.where(value));
        }
        @Test
        void testIsNotCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("name");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.where(value));
        }
    }

    @Nested
    class SizeTest {
        @Test
        void testNormal() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(false);
            when(value.isNumber()).thenReturn(false);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.size("", value);
                } catch (BizException e) {
                    Assertions.assertEquals("module.save.check.where.notNumber", e.getErrorCode());
                    throw e;
                }
            });
        }
        @Test
        void testIsCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.isNumber()).thenReturn(true);
            when(value.textValue()).thenReturn("{{name}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.size("", value));
        }
        @Test
        void testIsNotCustomParam() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.isNumber()).thenReturn(true);
            when(value.textValue()).thenReturn("name");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.size("", value));
        }
    }

    @Nested
    class ElementOrNotTest {
        @Test
        void testNormal() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("{{name}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.elementOrNot("", value, null));
        }
        @Test
        void testNotObject() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("name");
            when(value.isObject()).thenReturn(false);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.elementOrNot("", value, null);
                } catch (BizException e) {
                    Assertions.assertEquals("module.save.check.where.notObject", e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Nested
    class TextTest {
        @Test
        void testNormal() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("{{name}}");
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.text(value));
        }
        @Test
        void testNotObject() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("name");
            when(value.isObject()).thenReturn(false);
            when(value.has("$search")).thenReturn(true);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.text(value);
                } catch (BizException e) {
                    Assertions.assertEquals("module.save.check.where.notContainSearch", e.getErrorCode());
                    throw e;
                }
            });
        }
        @Test
        void testNot$search() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("name");
            when(value.isObject()).thenReturn(true);
            when(value.has("$search")).thenReturn(false);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.text(value);
                } catch (BizException e) {
                    Assertions.assertEquals("module.save.check.where.notContainSearch", e.getErrorCode());
                    throw e;
                }
            });
        }
        @Test
        void testAllNot() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("name");
            when(value.isObject()).thenReturn(false);
            when(value.has("$search")).thenReturn(false);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.text(value);
                } catch (BizException e) {
                    Assertions.assertEquals("module.save.check.where.notContainSearch", e.getErrorCode());
                    throw e;
                }
            });
        }

        @Test
        void testSearchNotTxt() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("name");
            when(value.isObject()).thenReturn(true);
            when(value.has("$search")).thenReturn(true);
            JsonNode search = mock(JsonNode.class);
            when(value.get("$search")).thenReturn(search);
            when(search.isTextual()).thenReturn(false);
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    MongoQueryValidator.text(value);
                } catch (BizException e) {
                    Assertions.assertEquals("module.save.check.where.notSearchString", e.getErrorCode());
                    throw e;
                }
            });
        }

        @Test
        void testSearchIsTxt() {
            JsonNode value = mock(JsonNode.class);
            when(value.isTextual()).thenReturn(true);
            when(value.textValue()).thenReturn("name");
            when(value.isObject()).thenReturn(true);
            when(value.has("$search")).thenReturn(true);
            JsonNode search = mock(JsonNode.class);
            when(value.get("$search")).thenReturn(search);
            when(search.isTextual()).thenReturn(true);
            Assertions.assertDoesNotThrow(() -> MongoQueryValidator.text(value));
        }
    }

    JsonNode parse(String json) {
        try {
            return new ObjectMapper().readTree(json);
        } catch (Exception e) {
            return null;
        }
    }
}