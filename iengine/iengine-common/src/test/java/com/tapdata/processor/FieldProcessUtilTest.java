package com.tapdata.processor;

import com.tapdata.entity.FieldProcess;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;

import static com.tapdata.processor.FieldProcessUtil.fieldProcess;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

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

    @Nested
    @DisplayName("Method fieldProcess test")
    class fieldProcessTest {

        private Map<String, Object> record;

        @BeforeEach
        void setUp() {
            record = new HashMap<>();
            record.put("id", 1);
            record.put("name", "Alice");
            record.put("age", 30);
            Map<String, Object> nestedMap = new HashMap<>();
            nestedMap.put("city", "New York");
            nestedMap.put("country", "USA");
            record.put("address", nestedMap);
        }

        @Test
        @DisplayName("test delete all fields")
        void test1() {
            Set<String> rollbackRemoveFields = new HashSet<>();
            rollbackRemoveFields.add("id");
            rollbackRemoveFields.add("name");
            assertDoesNotThrow(() -> fieldProcess(record, new ArrayList<>(), rollbackRemoveFields, true));
            assertEquals(2, record.size());
            assertEquals(1, record.get("id"));
            assertEquals("Alice", record.get("name"));
        }

        @Test
        @DisplayName("test remove fields")
        void test2() {
            List<FieldProcess> fieldProcesses = new ArrayList<>();
            FieldProcess fieldProcess = new FieldProcess();
            fieldProcess.setField("age");
            fieldProcess.setOp(FieldProcess.FieldOp.OP_REMOVE.getOperation());
            fieldProcesses.add(fieldProcess);
            FieldProcess fieldProcess1 = new FieldProcess();
            fieldProcess1.setField("address");
            fieldProcess1.setOp(FieldProcess.FieldOp.OP_REMOVE.getOperation());
            fieldProcesses.add(fieldProcess1);

            Set<String> rollbackRemoveFields = new HashSet<>();
            rollbackRemoveFields.add("age");

            assertDoesNotThrow(() -> fieldProcess(record, fieldProcesses, rollbackRemoveFields, false));
            assertEquals(3, record.size());
            assertEquals(1, record.get("id"));
            assertEquals("Alice", record.get("name"));
            assertEquals(30, record.get("age"));
        }
    }

    @Nested
    class convertTest{
        @Test
        @DisplayName("test when origin value is Long")
        void test1() {
            DateTime value = new DateTime(1730281800000L);
            String newDataType = "String";
            Object actual = FieldProcessUtil.convert(value, newDataType);
            assertEquals("2024-10-30T09:50:00Z", actual);
        }
    }
}
