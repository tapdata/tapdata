package com.tapdata.tm.commons.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.node.TextNode;
import io.tapdata.entity.schema.partition.type.TapPartitionRange;
import io.tapdata.entity.schema.partition.type.TapPartitionType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.lang.reflect.Type;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class JsonUtilTest {

    @Nested
    class TapPartitionTypeDeserializerTest {
        JsonUtil.TapPartitionTypeDeserializer deserializer;
        JsonParser p;
        DeserializationContext ctxt;
        ObjectCodec codec;
        TreeNode treeNode;
        TextNode partitionTypeNode;
        Class<? extends TapPartitionType> tapTypeClass;
        @BeforeEach
        void init() throws IOException {
            deserializer = mock(JsonUtil.TapPartitionTypeDeserializer.class);
            p = mock(JsonParser.class);
            ctxt = mock(DeserializationContext.class);
            codec = mock(ObjectCodec.class);
            treeNode = mock(TreeNode.class);
            partitionTypeNode = mock(TextNode.class);

            when(p.getCodec()).thenReturn(codec);
            when(codec.readTree(p)).thenReturn(treeNode);
            when(treeNode.get(TapPartitionType.KEY_NAME)).thenReturn(partitionTypeNode);
            when(partitionTypeNode.textValue()).thenReturn("type");
            when(codec.treeToValue(treeNode, tapTypeClass)).thenReturn(null);
            when(deserializer.deserialize(p, ctxt)).thenCallRealMethod();
        }
        @Test
        void testNormal() {
            try(MockedStatic<TapPartitionType> tpt = mockStatic(TapPartitionType.class)) {
                tpt.when(() -> TapPartitionType.getTapPartitionTypeClass(anyString())).thenReturn(TapPartitionRange.class);
                Assertions.assertDoesNotThrow(() -> deserializer.deserialize(p, ctxt));
            }
        }
        @Test
        void testTreeNodeIsNull() throws IOException {
            when(codec.readTree(p)).thenReturn(null);
            try(MockedStatic<TapPartitionType> tpt = mockStatic(TapPartitionType.class)) {
                tpt.when(() -> TapPartitionType.getTapPartitionTypeClass(anyString())).thenReturn(TapPartitionRange.class);
                Assertions.assertDoesNotThrow(() -> deserializer.deserialize(p, ctxt));
            }
        }
        @Test
        void testPartitionTypeNodeIsNull() throws IOException {
            when(treeNode.get(TapPartitionType.KEY_NAME)).thenReturn(null);
            try(MockedStatic<TapPartitionType> tpt = mockStatic(TapPartitionType.class)) {
                tpt.when(() -> TapPartitionType.getTapPartitionTypeClass(anyString())).thenReturn(TapPartitionRange.class);
                Assertions.assertDoesNotThrow(() -> deserializer.deserialize(p, ctxt));
            }
        }
        @Test
        void testTapTypeClassIsNull() throws IOException {
            try(MockedStatic<TapPartitionType> tpt = mockStatic(TapPartitionType.class)) {
                tpt.when(() -> TapPartitionType.getTapPartitionTypeClass(anyString())).thenReturn(null);
                Assertions.assertThrows(IllegalArgumentException.class, () -> deserializer.deserialize(p, ctxt));
            }
        }
    }

    @Nested
    class TapPartitionTypeObjectDeserializerTest {
        @Test
        void testNormal() {
            JsonUtil util = new JsonUtil();
            ObjectDeserializer deserializer = ParserConfig.getGlobalInstance().getDeserializer(TapPartitionType.class);
            DefaultJSONParser parser = mock(DefaultJSONParser.class);
            Type type = mock(Type.class);
            JSONObject jsonObject = mock(JSONObject.class);

            when(parser.parseObject(JSONObject.class)).thenReturn(jsonObject);
            when(jsonObject.getString(TapPartitionType.KEY_NAME)).thenReturn(TapPartitionType.RANGE);
            when(jsonObject.toJavaObject(TapPartitionRange.class)).thenReturn(mock(TapPartitionRange.class));
            Assertions.assertDoesNotThrow(() -> deserializer.deserialze(parser, type, "name"));
        }
        @Test
        void testJsonObjectIsNull() {
            JsonUtil util = new JsonUtil();
            ObjectDeserializer deserializer = ParserConfig.getGlobalInstance().getDeserializer(TapPartitionType.class);
            DefaultJSONParser parser = mock(DefaultJSONParser.class);
            Type type = mock(Type.class);
            JSONObject jsonObject = mock(JSONObject.class);

            when(parser.parseObject(JSONObject.class)).thenReturn(null);
            when(jsonObject.getString(TapPartitionType.KEY_NAME)).thenReturn(TapPartitionType.RANGE);
            when(jsonObject.toJavaObject(TapPartitionRange.class)).thenReturn(mock(TapPartitionRange.class));
            Assertions.assertDoesNotThrow(() -> deserializer.deserialze(parser, type, "name"));
            Assertions.assertEquals(0, deserializer.getFastMatchToken());
        }
        @Test
        void testTypeStrIsNull() {
            JsonUtil util = new JsonUtil();
            ObjectDeserializer deserializer = ParserConfig.getGlobalInstance().getDeserializer(TapPartitionType.class);
            DefaultJSONParser parser = mock(DefaultJSONParser.class);
            Type type = mock(Type.class);
            JSONObject jsonObject = mock(JSONObject.class);

            when(parser.parseObject(JSONObject.class)).thenReturn(jsonObject);
            when(jsonObject.getString(TapPartitionType.KEY_NAME)).thenReturn(null);
            when(jsonObject.toJavaObject(TapPartitionRange.class)).thenReturn(mock(TapPartitionRange.class));
            Assertions.assertDoesNotThrow(() -> deserializer.deserialze(parser, type, "name"));
        }
    }
}