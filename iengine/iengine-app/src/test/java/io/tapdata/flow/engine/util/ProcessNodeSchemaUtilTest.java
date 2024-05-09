package io.tapdata.flow.engine.util;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.JavaTypesToTapTypes;
import io.tapdata.observable.logging.ObsLogger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.tapArray;
import static io.tapdata.entity.simplify.TapSimplify.tapMap;
import static io.tapdata.entity.simplify.TapSimplify.tapString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProcessNodeSchemaUtilTest {


    @Nested
    @DisplayName("Test preserving sub attributes after model inference on Js nodes")
    class RetainedOldSubFields{
        TapTable tapTable;
        LinkedHashMap<String, TapField> oldNameFieldMap;
        TapField f;
        LinkedHashMap<String, TapField> nameFieldMap;
        TapField field;
        Map<String, Object> afterValue;
        @BeforeEach
        void init() {

            f = mock(TapField.class);
            field = mock(TapField.class);

            tapTable = new TapTable("id");
            nameFieldMap = new LinkedHashMap<>();
            nameFieldMap.put("f", f);
            when(f.getTapType()).thenReturn(new TapMap());
            tapTable.setNameFieldMap(nameFieldMap);

            oldNameFieldMap = new LinkedHashMap<>();
            oldNameFieldMap.put("f", field);
            when(field.getTapType()).thenReturn(new TapMap());
            afterValue = new HashMap<>();

        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> ProcessNodeSchemaUtil.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
            Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
            Assertions.assertNotNull(tapTable.getNameFieldMap().get("f"));
        }

        @Test
        void testOldNameFieldMapIsEmpty() {
            oldNameFieldMap.clear();
            Assertions.assertDoesNotThrow(() -> ProcessNodeSchemaUtil.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
            Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
            Assertions.assertNotNull(tapTable.getNameFieldMap().get("f"));
        }

        @Test
        void testContainsSubFieldButNotContainsSubField() {
            TapField subField = mock(TapField.class);
            when(subField.getName()).thenReturn("f.id");
            oldNameFieldMap.put("f.id", subField);
            afterValue.put("f", new HashMap<>());
            Assertions.assertDoesNotThrow(() -> ProcessNodeSchemaUtil.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
            Assertions.assertEquals(2, tapTable.getNameFieldMap().size());
            Assertions.assertNotNull(tapTable.getNameFieldMap().get("f.id"));
        }
        @Test
        void testContainsSubFieldAndContainsSubField() {
            TapField subField = mock(TapField.class);
            when(subField.getName()).thenReturn("f.id");
            oldNameFieldMap.put("f.id", subField);
            afterValue.put("f", new HashMap<>());
            afterValue.put("f.id", "id");
            Assertions.assertDoesNotThrow(() -> ProcessNodeSchemaUtil.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
            Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
            Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
        }

        @Test
        void testNotContainsSubFieldButNotContainsSubField() {
            TapField subField = mock(TapField.class);
            when(subField.getName()).thenReturn("f.id");
            oldNameFieldMap.put("f.id", subField);
            Assertions.assertDoesNotThrow(() -> ProcessNodeSchemaUtil.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
            Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
            Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
        }
        @Test
        void testOldNameFieldMapNotContainsFatherField() {
            TapField subField = mock(TapField.class);
            when(subField.getName()).thenReturn("f.id");
            oldNameFieldMap.put("f.id", subField);
            oldNameFieldMap.remove("f");
            afterValue.put("f", new HashMap<>());
            Assertions.assertDoesNotThrow(() -> ProcessNodeSchemaUtil.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
            Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
            Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
        }
        @Test
        void testTapTableNotContainsFatherField() {
            TapField subField = mock(TapField.class);
            when(subField.getName()).thenReturn("f.id");
            nameFieldMap.remove("f");
            oldNameFieldMap.put("f.id", subField);
            afterValue.put("f", new HashMap<>());
            Assertions.assertDoesNotThrow(() -> ProcessNodeSchemaUtil.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
            Assertions.assertEquals(0, tapTable.getNameFieldMap().size());
            Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
        }
        @Test
        void testTapTableContainsFatherFieldButFieldTapTypeIsNull() {
            TapField subField = mock(TapField.class);
            when(subField.getName()).thenReturn("f.id");
            when(f.getTapType()).thenReturn(null);
            oldNameFieldMap.put("f.id", subField);
            afterValue.put("f", new HashMap<>());
            Assertions.assertDoesNotThrow(() -> ProcessNodeSchemaUtil.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
            Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
            Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
        }
        @Test
        void testBeforeTapNotContainsAfterTap() {
            TapField subField = mock(TapField.class);
            when(subField.getName()).thenReturn("f.id");
            when(f.getTapType()).thenReturn(new TapArray());
            oldNameFieldMap.put("f.id", subField);
            afterValue.put("f", new HashMap<>());
            Assertions.assertDoesNotThrow(() -> ProcessNodeSchemaUtil.retainedOldSubFields(tapTable, oldNameFieldMap, afterValue));
            Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
            Assertions.assertNull(tapTable.getNameFieldMap().get("f.id"));
        }
    }

    @Nested
    @DisplayName("")
    class ScanTapFieldTest {
        ObsLogger obsLogger;
        TapTable tapTable;
        Map<String, TapField> oldNameFieldMap;
        @BeforeEach
        void init() {
            tapTable = new TapTable();
            obsLogger = mock(ObsLogger.class);
            when(obsLogger.isDebugEnabled()).thenReturn(true);
            doNothing().when(obsLogger).debug(anyString(), anyString(), any(Class.class));
            doNothing().when(obsLogger).debug(anyString(), anyString(), anyString());
            oldNameFieldMap = new LinkedHashMap<>();
        }
        @Nested
        class NormalFieldTest {
            @Test
            void testNormalField() {
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", "value", obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(1)).debug(anyString(), anyString(), any(Class.class));
            }

            @Test
            void testNotSupportDebug() {
                when(obsLogger.isDebugEnabled()).thenReturn(false);
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", "value", obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(0)).debug(anyString(), anyString(), any(Class.class));
            }
            @Test
            void testTapRow() {
                when(obsLogger.isDebugEnabled()).thenReturn(false);
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", null, obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(0)).debug(anyString(), anyString(), any(Class.class));
            }

            @Test
            void testOldNameFieldMapIsEmpty() {
                ProcessNodeSchemaUtil.scanTapField(tapTable, null, "key", "value", obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(1)).debug(anyString(), anyString(), any(Class.class));
            }

            @Test
            void testOldNameFieldMapContainsField() {
                oldNameFieldMap.put("key", new TapField().name("key").tapType(JavaTypesToTapTypes.toTapType(JavaTypesToTapTypes.JAVA_String)));
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", "value", obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(1)).debug(anyString(), anyString(), any(Class.class));
            }
            @Test
            void testOldNameFieldMapContainsFieldButOldTapTypeIsNull() {
                oldNameFieldMap.put("key", new TapField().name("key"));
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", "value", obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(1)).debug(anyString(), anyString(), any(Class.class));
            }
            @Test
            void testOldNameFieldMapContainsFieldButOldTapTypeNotEqualsTapType() {
                oldNameFieldMap.put("key", new TapField().name("key").tapType(JavaTypesToTapTypes.toTapType(JavaTypesToTapTypes.JAVA_Double)));
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", "value", obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(1)).debug(anyString(), anyString(), any(Class.class));
            }
            @Test
            void testOldNameFieldMapContainsFieldButTapTypeIsTapRow() {
                oldNameFieldMap.put("key", new TapField().name("key").tapType(JavaTypesToTapTypes.toTapType(JavaTypesToTapTypes.JAVA_Double)));
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", null, obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(1)).debug(anyString(), anyString(), anyString());
            }
            @Test
            void testTapValue() {
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", new TapStringValue("value").tapType(tapString().bytes(1024L)), obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(1)).debug(anyString(), anyString(), any(Class.class));
            }
        }

        @Nested
        class MapFieldTest {
            @Test
            void testNormalMapField() {
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", new HashMap<>(), obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(1)).debug(anyString(), anyString(), any(Class.class));
            }

            @Test
            void testTapMap() {
                Map<String, Object> map = new HashMap<>();
                map.put("key", "sub");
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", map, obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(2, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key.key"));
                verify(obsLogger, times(2)).debug(anyString(), anyString(), any(Class.class));
            }
            @Test
            void testTapMapValue() {
                TapMapValue tapMapValue = new TapMapValue();
                Map<String, Object> map = new HashMap<>();
                map.put("key", "sub");
                tapMapValue.setValue(map);
                tapMapValue.setTapType(tapMap());
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", tapMapValue, obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(2, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key.key"));
                verify(obsLogger, times(2)).debug(anyString(), anyString(), any(Class.class));
            }
        }

        @Nested
        class ArrayTest {
            @Test
            void testNormalMapField() {
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", new ArrayList<>(), obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(1, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                verify(obsLogger, times(1)).debug(anyString(), anyString(), any(Class.class));
            }

            @Test
            void testTapArray() {
                List<Object> list = new ArrayList<>();
                list.add("1");
                Map<String, Object> map = new HashMap<>();
                map.put("key", "sub");
                list.add(map);
                TapArrayValue tapArrayValue = new TapArrayValue();
                tapArrayValue.setValue(list);
                tapArrayValue.setTapType(tapArray());
                ProcessNodeSchemaUtil.scanTapField(tapTable, oldNameFieldMap, "key", tapArrayValue, obsLogger);
                Assertions.assertNotNull(tapTable.getNameFieldMap());
                Assertions.assertEquals(2, tapTable.getNameFieldMap().size());
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key"));
                Assertions.assertNotNull(tapTable.getNameFieldMap().get("key.key"));
                verify(obsLogger, times(2)).debug(anyString(), anyString(), any(Class.class));
            }
        }
    }
}