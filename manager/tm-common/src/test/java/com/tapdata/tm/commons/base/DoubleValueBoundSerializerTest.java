package com.tapdata.tm.commons.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DoubleValueBoundSerializerTest {

    @Mock
    private JsonGenerator jsonGenerator;

    @Mock
    private SerializerProvider serializerProvider;

    @Mock
    private BeanProperty beanProperty;

    @Mock
    private DoubleValueBound doubleValueBound;

    private DoubleValueBoundSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new DoubleValueBoundSerializer();
    }

    @Test
    void testSerializeNullValue() throws IOException {
        serializer.serialize(null, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeNull();
        verifyNoMoreInteractions(jsonGenerator);
    }

    @Test
    void testSerializeDoubleWithinBounds() throws IOException {
        serializer.min = 0D;
        serializer.max = 100D;
        serializer.serialize(50.5D, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeNumber(50.5D);
    }

    @Test
    void testSerializeDoubleBelowMin() throws IOException {
        serializer.min = 10D;
        serializer.max = 100D;
        serializer.serialize(5D, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeNumber(10D);
    }

    @Test
    void testSerializeDoubleAboveMax() throws IOException {
        serializer.min = 10D;
        serializer.max = 100D;
        serializer.serialize(101D, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeNumber(100D);
    }

    @Test
    void testSerializeDoubleWithMinGreaterThanMax() throws IOException {
        serializer.min = 100D;
        serializer.max = 0D;
        serializer.serialize(200D, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeNumber(100D);
    }

    @Test
    void testSerializeNaNValue() throws IOException {
        serializer.serialize(Double.NaN, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeNull();
    }

    @Test
    void testSerializeInfiniteValue() throws IOException {
        serializer.serialize(Double.POSITIVE_INFINITY, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeNull();
    }

    @Test
    void testSerializeListWithNumberValues() throws IOException {
        serializer.min = 10D;
        serializer.max = 100D;
        List<Object> list = Arrays.asList(5D, 50.5D, null, 101D);
        serializer.serialize(list, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeStartArray();
        verify(jsonGenerator).writeNumber(10D);
        verify(jsonGenerator).writeNumber(50.5D);
        verify(jsonGenerator).writeNull();
        verify(jsonGenerator).writeNumber(100D);
        verify(jsonGenerator).writeEndArray();
    }

    @Test
    void testSerializeListWithMixedValues() throws IOException {
        serializer.min = 10D;
        serializer.max = 100D;
        List<Object> list = Arrays.asList(5D, "string", 20, null);
        serializer.serialize(list, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeStartArray();
        verify(jsonGenerator).writeNumber(10D);
        verify(jsonGenerator).writeObject("string");
        verify(jsonGenerator).writeNumber(20D);
        verify(jsonGenerator).writeNull();
        verify(jsonGenerator).writeEndArray();
    }

    @Test
    void testSerializeEmptyList() throws IOException {
        List<Object> emptyList = Collections.emptyList();
        serializer.serialize(emptyList, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeStartArray();
        verify(jsonGenerator).writeEndArray();
        verifyNoMoreInteractions(jsonGenerator);
    }

    @Test
    void testCreateContextualWithNullProperty() throws Exception {
        JsonSerializer<?> result = serializer.createContextual(serializerProvider, null);
        assertSame(serializer, result);
    }

    @Test
    void testCreateContextualWithPropertyButNoAnnotation() throws Exception {
        when(beanProperty.getAnnotation(DoubleValueBound.class)).thenReturn(null);
        JsonSerializer<?> result = serializer.createContextual(serializerProvider, beanProperty);
        assertSame(serializer, result);
    }

    @Test
    void testCreateContextualWithAnnotation() throws Exception {
        when(beanProperty.getAnnotation(DoubleValueBound.class)).thenReturn(doubleValueBound);
        when(doubleValueBound.min()).thenReturn(1D);
        when(doubleValueBound.max()).thenReturn(2D);

        JsonSerializer<?> result = serializer.createContextual(serializerProvider, beanProperty);
        assertNotSame(serializer, result);
        assertTrue(result instanceof DoubleValueBoundSerializer);

        DoubleValueBoundSerializer newSerializer = (DoubleValueBoundSerializer) result;
        assertEquals(1D, newSerializer.min);
        assertEquals(2D, newSerializer.max);
    }
}
