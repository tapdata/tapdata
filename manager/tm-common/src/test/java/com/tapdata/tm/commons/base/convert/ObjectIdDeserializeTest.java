package com.tapdata.tm.commons.base.convert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.node.IntNode;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ObjectIdDeserializeTest {
    @Test
    void test_deserialize() throws IOException {
        ObjectIdDeserialize objectIdDeserialize = new ObjectIdDeserialize();
        JsonParser jsonParser = mock(JsonParser.class);
        when(jsonParser.getValueAsString()).thenReturn(null);
        ObjectCodec codec = mock(ObjectCodec.class);
        TreeNode treeNode = mock(TreeNode.class);
        when(codec.readTree(any())).thenReturn(treeNode);
        IntNode timestampNode = mock(IntNode.class);
        IntNode counterNode = mock(IntNode.class);
        when(treeNode.get("timestamp")).thenReturn(timestampNode);
        when(treeNode.get("counter")).thenReturn(counterNode);
        when(timestampNode.numberValue()).thenReturn(123456);
        when(counterNode.numberValue()).thenReturn(123);
        when(jsonParser.getCodec()).thenReturn(codec);
        ObjectId result = objectIdDeserialize.deserialize(jsonParser,mock(DeserializationContext.class));
        Assertions.assertEquals(123456,result.getTimestamp());
    }

    @Test
    void test_timestampIsnull() throws IOException {
        ObjectIdDeserialize objectIdDeserialize = new ObjectIdDeserialize();
        JsonParser jsonParser = mock(JsonParser.class);
        when(jsonParser.getValueAsString()).thenReturn(null);
        ObjectCodec codec = mock(ObjectCodec.class);
        TreeNode treeNode = mock(TreeNode.class);
        when(codec.readTree(any())).thenReturn(treeNode);
        when(treeNode.get("timestamp")).thenReturn(null);
        when(jsonParser.getCodec()).thenReturn(codec);
        ObjectId result = objectIdDeserialize.deserialize(jsonParser,mock(DeserializationContext.class));
        Assertions.assertNull(result);
    }
    @Test
    void test_counterIsnull() throws IOException {
        ObjectIdDeserialize objectIdDeserialize = new ObjectIdDeserialize();
        JsonParser jsonParser = mock(JsonParser.class);
        when(jsonParser.getValueAsString()).thenReturn(null);
        ObjectCodec codec = mock(ObjectCodec.class);
        TreeNode treeNode = mock(TreeNode.class);
        when(codec.readTree(any())).thenReturn(treeNode);
        IntNode timestampNode = mock(IntNode.class);
        when(treeNode.get("timestamp")).thenReturn(timestampNode);
        when(treeNode.get("counter")).thenReturn(null);
        when(timestampNode.numberValue()).thenReturn(123456);
        when(jsonParser.getCodec()).thenReturn(codec);
        ObjectId result = objectIdDeserialize.deserialize(jsonParser,mock(DeserializationContext.class));
        Assertions.assertNull(result);
    }
}
