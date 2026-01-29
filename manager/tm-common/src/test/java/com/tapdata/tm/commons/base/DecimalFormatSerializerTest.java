package com.tapdata.tm.commons.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DecimalFormatSerializerTest {

    @Mock
    private JsonGenerator jsonGenerator;

    @Mock
    private SerializerProvider serializerProvider;

    @Mock
    private BeanProperty beanProperty;

    @Mock
    private DecimalFormat decimalFormat;

    private DecimalFormatSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new DecimalFormatSerializer();
    }

    @Test
    void testSerializeNullValue() throws IOException {
        serializer.serialize(null, jsonGenerator, serializerProvider);
        
        verify(jsonGenerator).writeNull();
        verifyNoMoreInteractions(jsonGenerator);
    }

    @Test
    void testSerializeDoubleValue() throws IOException {
        Double value = 123.456789;
        
        serializer.serialize(value, jsonGenerator, serializerProvider);
        
        verify(jsonGenerator).writeNumber(eq(BigDecimal.valueOf(123.456789).setScale(2, RoundingMode.HALF_UP)));
    }

    @Test
    void testSerializeDoubleValueWithCustomScale() throws IOException {
        // Create serializer with custom scale
        DecimalFormatSerializer customSerializer = new DecimalFormatSerializer();
        customSerializer.scale = 3;
        customSerializer.roundingMode = RoundingMode.DOWN;
        
        Double value = 123.456789;
        
        customSerializer.serialize(value, jsonGenerator, serializerProvider);
        
        verify(jsonGenerator).writeNumber(eq(BigDecimal.valueOf(123.456789).setScale(3, RoundingMode.DOWN)));
    }

    @Test
    void testSerializeListWithDoubleValues() throws IOException {
        List<Object> list = Arrays.asList(123.456, 789.123, null, 456.789);
        
        serializer.serialize(list, jsonGenerator, serializerProvider);
        
        verify(jsonGenerator).writeStartArray();
        verify(jsonGenerator).writeNumber(eq(BigDecimal.valueOf(123.456).setScale(2, RoundingMode.HALF_UP)));
        verify(jsonGenerator).writeNumber(eq(BigDecimal.valueOf(789.123).setScale(2, RoundingMode.HALF_UP)));
        verify(jsonGenerator).writeNull();
        verify(jsonGenerator).writeNumber(eq(BigDecimal.valueOf(456.789).setScale(2, RoundingMode.HALF_UP)));
        verify(jsonGenerator).writeEndArray();
    }

    @Test
    void testSerializeListWithMixedValues() throws IOException {
        List<Object> list = Arrays.asList(123.456, "string", 789, null);
        
        serializer.serialize(list, jsonGenerator, serializerProvider);
        
        verify(jsonGenerator).writeStartArray();
        verify(jsonGenerator).writeNumber(eq(BigDecimal.valueOf(123.456).setScale(2, RoundingMode.HALF_UP)));
        verify(jsonGenerator).writeObject("string");
        verify(jsonGenerator).writeObject(789);
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
    void testSerializeListWithException() throws IOException {
        List<Object> list = Arrays.asList(123.456);
        doThrow(new IOException("Test exception")).when(jsonGenerator).writeNumber(any(BigDecimal.class));
        
        assertThrows(IOException.class, () -> {
            serializer.serialize(list, jsonGenerator, serializerProvider);
        });
        
        verify(jsonGenerator).writeStartArray();
        verify(jsonGenerator).writeEndArray();
    }

    @Test
    void testSerializeNonDoubleNonListValue() throws IOException {
        String value = "test string";
        
        serializer.serialize(value, jsonGenerator, serializerProvider);
        
        verify(jsonGenerator).writeObject("test string");
    }

    @Test
    void testSerializeIntegerValue() throws IOException {
        Integer value = 123;
        
        serializer.serialize(value, jsonGenerator, serializerProvider);
        
        verify(jsonGenerator).writeObject(123);
    }

    @Test
    void testCreateContextualWithNullProperty() throws JsonMappingException {
        JsonSerializer<?> result = serializer.createContextual(serializerProvider, null);
        
        assertSame(serializer, result);
    }

    @Test
    void testCreateContextualWithPropertyButNoAnnotation() throws JsonMappingException {
        when(beanProperty.getAnnotation(DecimalFormat.class)).thenReturn(null);
        
        JsonSerializer<?> result = serializer.createContextual(serializerProvider, beanProperty);
        
        assertSame(serializer, result);
    }

    @Test
    void testCreateContextualWithAnnotation() throws JsonMappingException {
        when(beanProperty.getAnnotation(DecimalFormat.class)).thenReturn(decimalFormat);
        when(decimalFormat.scale()).thenReturn(4);
        when(decimalFormat.roundingMode()).thenReturn(RoundingMode.CEILING);
        
        JsonSerializer<?> result = serializer.createContextual(serializerProvider, beanProperty);
        
        assertNotSame(serializer, result);
        assertTrue(result instanceof DecimalFormatSerializer);
        
        DecimalFormatSerializer newSerializer = (DecimalFormatSerializer) result;
        assertEquals(4, newSerializer.scale);
        assertEquals(RoundingMode.CEILING, newSerializer.roundingMode);
    }

    @Test
    void testCreateContextualWithDefaultAnnotationValues() throws JsonMappingException {
        when(beanProperty.getAnnotation(DecimalFormat.class)).thenReturn(decimalFormat);
        when(decimalFormat.scale()).thenReturn(2);
        when(decimalFormat.roundingMode()).thenReturn(RoundingMode.HALF_UP);
        
        JsonSerializer<?> result = serializer.createContextual(serializerProvider, beanProperty);
        
        assertNotSame(serializer, result);
        assertTrue(result instanceof DecimalFormatSerializer);
        
        DecimalFormatSerializer newSerializer = (DecimalFormatSerializer) result;
        assertEquals(2, newSerializer.scale);
        assertEquals(RoundingMode.HALF_UP, newSerializer.roundingMode);
    }

    @Test
    void testSerializeWithDifferentRoundingModes() throws IOException {
        Double value = 123.125;
        
        // Test HALF_UP (default)
        serializer.serialize(value, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeNumber(eq(BigDecimal.valueOf(123.125).setScale(2, RoundingMode.HALF_UP)));
        
        reset(jsonGenerator);
        
        // Test HALF_DOWN
        DecimalFormatSerializer halfDownSerializer = new DecimalFormatSerializer();
        halfDownSerializer.roundingMode = RoundingMode.HALF_DOWN;
        halfDownSerializer.serialize(value, jsonGenerator, serializerProvider);
        verify(jsonGenerator).writeNumber(eq(BigDecimal.valueOf(123.125).setScale(2, RoundingMode.HALF_DOWN)));
    }

    @Test
    void testDefaultValues() {
        DecimalFormatSerializer newSerializer = new DecimalFormatSerializer();
        assertEquals(2, newSerializer.scale);
        assertEquals(RoundingMode.HALF_UP, newSerializer.roundingMode);
    }
}