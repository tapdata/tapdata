package com.tapdata.processor;

import io.tapdata.entity.schema.value.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FieldProcessUtilTest {
    @Nested
    class convertNumber{
        private Object value;
        private Function fn;
        @BeforeEach
        void beforeEach(){
            fn = mock(Function.class);
        }
        @Test
        @DisplayName("test convertNumber method for blank str")
        void test1(){
            value = "";
            FieldProcessUtil.convertNumber(value, fn);
            verify(fn, new Times(0)).apply(anyString());
        }
        @Test
        @DisplayName("test convertNumber method for int")
        void test2(){
            value = 1;
            FieldProcessUtil.convertNumber(value, fn);
            verify(fn, new Times(1)).apply(anyString());
        }
    }
    @Nested
    class handleDateTime{
        @Test
        void testForDateTime(){
            Long expect = 1718953878000L;
            Object value = new DateTime(expect);
            Object actual = FieldProcessUtil.handleDateTime(value);
            assertEquals(expect, actual);
        }
        @Test
        void testForLong(){
            Object value = 1L;
            Object actual = FieldProcessUtil.handleDateTime(value);
            assertEquals(value, actual);
        }
    }
}
