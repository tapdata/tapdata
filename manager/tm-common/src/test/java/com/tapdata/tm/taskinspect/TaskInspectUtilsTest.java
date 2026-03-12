package com.tapdata.tm.taskinspect;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.schema.value.*;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/15 17:22 Create
 */
@ExtendWith(MockitoExtension.class)
class TaskInspectUtilsTest {

    @Mock
    AutoCloseable closeable1;
    @Mock
    AutoCloseable closeable2;
    @Mock
    BooleanSupplier stopSupplier;

    @BeforeEach
    void setUp() {
        reset(closeable1, closeable2, stopSupplier);
    }

    @Nested
    class closeTest {

        @Test
        void testClose_NoCloseables() throws Exception {
            TaskInspectUtils.close();
            verifyNoInteractions(closeable1, closeable2);
        }

        @Test
        void testClose_OneCloseable() throws Exception {
            TaskInspectUtils.close(closeable1);
            verify(closeable1).close();
            verifyNoInteractions(closeable2);
        }

        @Test
        void testClose_MultipleCloseables() throws Exception {
            TaskInspectUtils.close(closeable1, closeable2);
            verify(closeable1).close();
            verify(closeable2).close();
        }

        @Test
        void testClose_ExceptionInFirstCloseable() throws Exception {
            Exception e1 = new Exception("Exception in closeable1");
            doThrow(e1).when(closeable1).close();
            Exception e2 = new Exception("Exception in closeable2");
            doThrow(e2).when(closeable2).close();

            Exception thrown = assertThrows(Exception.class, () -> TaskInspectUtils.close(closeable1, closeable2));
            assertEquals(e1, thrown);
            assertTrue(thrown.getSuppressed()[0] == e2);
        }

        @Test
        void testClose_ExceptionInSecondCloseable() throws Exception {
            Exception e2 = new Exception("Exception in closeable2");
            doThrow(e2).when(closeable2).close();

            Exception thrown = assertThrows(Exception.class, () -> TaskInspectUtils.close(closeable1, closeable2));
            assertEquals(e2, thrown);
            verify(closeable1).close();
        }
    }

    @Nested
    class stopTest {

        @Test
        void testStop_AlreadyStopped() throws InterruptedException {
            when(stopSupplier.getAsBoolean()).thenReturn(true);

            TaskInspectUtils.stop(1000, stopSupplier);

            verify(stopSupplier).getAsBoolean();
        }

        @Test
        void testStop_StopWithinTimeout() throws InterruptedException {
            when(stopSupplier.getAsBoolean()).thenReturn(false, false, true);

            TaskInspectUtils.stop(3000, stopSupplier);

            verify(stopSupplier, times(3)).getAsBoolean();
        }

        @Test
        void testStop_Timeout() throws InterruptedException {
            when(stopSupplier.getAsBoolean()).thenReturn(false);

            Exception thrown = assertThrows(RuntimeException.class, () -> TaskInspectUtils.stop(1000, stopSupplier));
            assertNotNull(thrown.getMessage());
            assertTrue(thrown.getMessage().startsWith("Timeout waiting"));

            // 1000ms / 500ms (sleep time) = 2
            // 允许调用2-3次，避免边界条件问题
            verify(stopSupplier, atLeast(2)).getAsBoolean();
            verify(stopSupplier, atMost(3)).getAsBoolean();
        }
    }

    @Nested
    class submitTest {

        @Test
        void testSubmit_Runnable() throws ExecutionException, InterruptedException {
            Runnable runnable = mock(Runnable.class);
            Future<?> future = TaskInspectUtils.submit(runnable);

            assertNotNull(future);
            future.get(); // This will block until the task is complete
            verify(runnable).run();
        }
    }

    @Nested
    class KeysSerializerTest {

        long currentTimeMillis;
        LinkedHashMap<String, Object> keys;
        List<TypeCheckItem<?>> typeCheckItems;

        <T> void appendCheckItem(T value) {
            appendCheckItem(value, (item, checkValue) -> {
                if (null == item.value) {
                    return null == checkValue;
                }
                String fromJsonStr = JSON.toJSONString(item.value);
                String toJsonStr = JSON.toJSONString(checkValue);
//                System.out.printf("compare '%s' from: %s, to %s\n", item.type, item.value, checkValue);
                return fromJsonStr.equals(toJsonStr);
            });
        }

        <T> void appendCheckItem(T value, BiPredicate<TypeCheckItem<T>, Object> compare) {
            TypeCheckItem<T> item = TypeCheckItem.of(value, compare);
            keys.put(item.type, item.value);
            typeCheckItems.add(item);
        }

        @BeforeEach
        void setUp() {
            currentTimeMillis = System.currentTimeMillis();
            keys = new LinkedHashMap<>();
            typeCheckItems = new ArrayList<>();

            keys.put("null", null);

            appendCheckItem(null);

            // 数字检查
            appendCheckItem(1);
            appendCheckItem(1.1f);
            appendCheckItem(1.1d);
            appendCheckItem(new BigDecimal("1.1"));

            // 时间检查
            appendCheckItem(currentTimeMillis);
            appendCheckItem(Instant.now());
            appendCheckItem(new Timestamp(currentTimeMillis));
            appendCheckItem(new Date(currentTimeMillis));
            appendCheckItem(new java.sql.Date(currentTimeMillis));

            // 集合 + 顺序 检查
            appendCheckItem(Optional.of(new HashMap<>()).map(m -> {
                m.put("4", "fourth");
                m.put("1", "first");
                m.put("tap-value-test", new TapDateTimeValue(DateTime.withDateStr("2025-01-01")));
                m.put("3", "third");
                m.put("2", "second");
                return m;
            }).get());
            appendCheckItem(Optional.of(new LinkedHashMap<>()).map(m -> {
                m.put("first", "1");
                m.put("second", "2");
                m.put("third", "3");
                m.put("tap-value-test", new TapDateTimeValue(DateTime.withDateStr("2025-01-01")));
                return m;
            }).get());
            appendCheckItem(Optional.of(new HashSet<>()).map(m -> {
                m.add("3");
                m.add(new TapDateTimeValue(DateTime.withDateStr("2025-01-01")));
                m.add("1");
                m.add(new TapNumberValue(5.0));
                m.add("2");
                m.add("4");
                return m;
            }).get(), (i, v) -> {
                if (null == v) return false;
                String fromJsonStr = String.join("::", i.value.stream().map(JSON::toJSONString).sorted().toList());
                String toJsonStr = String.join("::", i.value.stream().map(JSON::toJSONString).sorted().toList());
                return fromJsonStr.equals(toJsonStr);
            });

            // TapValue 检查
            appendCheckItem(new TapArrayValue(Optional.of(new ArrayList<>()).map(o -> {
                o.add("1");
                o.add("2");
                return o;
            }).get()));
            appendCheckItem(new TapBinaryValue("中文".getBytes()));
            appendCheckItem(new TapBooleanValue(true));
            appendCheckItem(new TapDateTimeValue(DateTime.withDateStr("2025-01-01")));
            appendCheckItem(new TapDateValue(DateTime.withDateStr("2025-01-01")));
//            appendCheckItem(new TapInputStreamValue(new ByteArrayInputStream("中文".getBytes()))); // 关联键不可能有此类型，如果有需要从关联键中排除
            appendCheckItem(new TapJsonValue("{\"id\":1,\"title\":\"中文\"}"));
            appendCheckItem(new TapMapValue(Optional.of(new LinkedHashMap<String, Object>()).map(m -> {
                m.put("id", 1);
                m.put("title", "中文");
                return m;
            }).get()));
            appendCheckItem(new TapMoneyValue(1.1));
            appendCheckItem(new TapNumberValue(1.1));
            appendCheckItem(new TapRawValue("中文".getBytes()));
            appendCheckItem(new TapStringValue("中文"));
            appendCheckItem(new TapTimeValue(DateTime.withTimeStr("00:00:01")));
            appendCheckItem(new TapXmlValue("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<root><title>中文</title></root>"));
            appendCheckItem(new TapYearValue(1));

            // 其它
            appendCheckItem("中文");
            appendCheckItem("中文".getBytes());
        }

        @Test
        void testCurrentVersion() {
            String encodeStr = assertDoesNotThrow(() -> TaskInspectUtils.keysSerialization(keys), "encodeKeys failed");
            LinkedHashMap<String, Object> decodeKeys = assertDoesNotThrow(() -> TaskInspectUtils.keysDeserialization(encodeStr, null), "decodeKeys failed");

            assertNotNull(decodeKeys);

            // 检查 key 顺序
            String fromKeySortStr = String.join("::", typeCheckItems.stream().map(o -> o.type).toList());
            String toKeySortStr = String.join("::", decodeKeys.keySet());
            assertEquals(fromKeySortStr, toKeySortStr);

            // 检查所有类型的序列化
            boolean isOk = true;
            for (TypeCheckItem item : typeCheckItems) {
                Object value = decodeKeys.get(item.type);
                if (!item.compare.test(item, value)) {
                    isOk = false;
                    System.err.printf("compare item '%s' failed\n- from: %s\n-   to: %s\n", item.type, item.value, value);
                }
            }

            String fromJson = JSON.toJSONString(keys);
            String toJson = JSON.toJSONString(decodeKeys);
            String errorTips = String.format("The keys are inconsistent\n- encode: %s\n-   from: %s\n-     to: %s\n", encodeStr, fromJson, toJson);
//            System.out.println(errorTips); // 用于输出当前版本的序列化信息，添加到 testHistoryVersions 中
            assertTrue(isOk, errorTips);
        }

        @Test
        void testHistoryVersions() {
            // 旧版兼容性判断
            List<VersionCheckItem> checkVersionList = List.of(
                VersionCheckItem.of("4.9.0"
                    , "{\"java.lang.Integer\":1,\"java.lang.Float\":1.1,\"java.lang.Double\":1.1,\"java.math.BigDecimal\":1.1,\"java.lang.Long\":1763532982708,\"java.time.Instant\":\"2025-11-19T06:16:22.708977Z\",\"java.sql.Timestamp\":1763532982708,\"java.util.Date\":1763532982708,\"java.sql.Date\":1763532982708,\"java.lang.String\":\"中文\",\"[B\":\"5Lit5paH\"}"
                    , "rO0ABXNyABdqYXZhLnV0aWwuTGlua2VkSGFzaE1hcDTATlwQbMD7AgABWgALYWNjZXNzT3JkZXJ4cgARamF2YS51dGlsLkhhc2hNYXAFB9rBwxZg0QMAAkYACmxvYWRGYWN0b3JJAAl0aHJlc2hvbGR4cD9AAAAAAAAMdwgAAAAQAAAADHQABG51bGxwdAARamF2YS5sYW5nLkludGVnZXJzcgARamF2YS5sYW5nLkludGVnZXIS4qCk94GHOAIAAUkABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwAAAAAXQAD2phdmEubGFuZy5GbG9hdHNyAA9qYXZhLmxhbmcuRmxvYXTa7cmi2zzw7AIAAUYABXZhbHVleHEAfgAGP4zMzXQAEGphdmEubGFuZy5Eb3VibGVzcgAQamF2YS5sYW5nLkRvdWJsZYCzwkopa/sEAgABRAAFdmFsdWV4cQB+AAY/8ZmZmZmZmnQAFGphdmEubWF0aC5CaWdEZWNpbWFsc3IAFGphdmEubWF0aC5CaWdEZWNpbWFsVMcVV/mBKE8DAAJJAAVzY2FsZUwABmludFZhbHQAFkxqYXZhL21hdGgvQmlnSW50ZWdlcjt4cQB+AAYAAAABc3IAFGphdmEubWF0aC5CaWdJbnRlZ2VyjPyfH6k7+x0DAAZJAAhiaXRDb3VudEkACWJpdExlbmd0aEkAE2ZpcnN0Tm9uemVyb0J5dGVOdW1JAAxsb3dlc3RTZXRCaXRJAAZzaWdudW1bAAltYWduaXR1ZGV0AAJbQnhxAH4ABv///////////////v////4AAAABdXIAAltCrPMX+AYIVOACAAB4cAAAAAELeHh0AA5qYXZhLmxhbmcuTG9uZ3NyAA5qYXZhLmxhbmcuTG9uZzuL5JDMjyPfAgABSgAFdmFsdWV4cQB+AAYAAAGamsHJtHQAEWphdmEudGltZS5JbnN0YW50c3IADWphdmEudGltZS5TZXKVXYS6GyJIsgwAAHhwdw0CAAAAAGkdYLYqQiFoeHQAEmphdmEuc3FsLlRpbWVzdGFtcHNyABJqYXZhLnNxbC5UaW1lc3RhbXAmGNXIAVO/ZQIAAUkABW5hbm9zeHIADmphdmEudXRpbC5EYXRlaGqBAUtZdBkDAAB4cHcIAAABmprBxvB4KjM5AHQADmphdmEudXRpbC5EYXRlc3EAfgAfdwgAAAGamsHJtHh0AA1qYXZhLnNxbC5EYXRlc3IADWphdmEuc3FsLkRhdGUU+kZoPzVmlwIAAHhxAH4AH3cIAAABmprBybR4dAAQamF2YS5sYW5nLlN0cmluZ3QABuS4reaWh3EAfgATdXEAfgAVAAAABuS4reaWh3gA"
                ),
                VersionCheckItem.of("4.10.0"
                    , "{\"java.lang.Integer\":1,\"java.lang.Float\":1.1,\"java.lang.Double\":1.1,\"java.math.BigDecimal\":1.1,\"java.lang.Long\":1763543705463,\"java.time.Instant\":\"2025-11-19T09:15:05.463698Z\",\"java.sql.Timestamp\":1763543705463,\"java.util.Date\":1763543705463,\"java.sql.Date\":1763543705463,\"io.tapdata.entity.schema.value.TapArrayValue\":{\"value\":[\"1\",\"2\"]},\"io.tapdata.entity.schema.value.TapBinaryValue\":{\"value\":{\"type\":0,\"value\":\"5Lit5paH\"}},\"io.tapdata.entity.schema.value.TapBooleanValue\":{\"value\":true},\"io.tapdata.entity.schema.value.TapDateTimeValue\":{\"value\":{\"containsIllegal\":false,\"fraction\":3,\"nano\":0,\"originType\":90,\"seconds\":1735660800}},\"io.tapdata.entity.schema.value.TapDateValue\":{\"value\":{\"containsIllegal\":false,\"fraction\":3,\"nano\":0,\"originType\":90,\"seconds\":1735660800}},\"io.tapdata.entity.schema.value.TapJsonValue\":{\"value\":\"{\\\"id\\\":1,\\\"title\\\":\\\"中文\\\"}\"},\"io.tapdata.entity.schema.value.TapMapValue\":{\"value\":{\"id\":1,\"title\":\"中文\"}},\"io.tapdata.entity.schema.value.TapMoneyValue\":{\"value\":1.1},\"io.tapdata.entity.schema.value.TapNumberValue\":{\"value\":1.1},\"io.tapdata.entity.schema.value.TapRawValue\":{\"value\":\"5Lit5paH\"},\"io.tapdata.entity.schema.value.TapStringValue\":{\"value\":\"中文\"},\"io.tapdata.entity.schema.value.TapTimeValue\":{\"value\":{\"containsIllegal\":false,\"fraction\":3,\"nano\":0,\"originType\":1,\"seconds\":1}},\"io.tapdata.entity.schema.value.TapXmlValue\":{\"value\":\"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>\\n<root><title>中文</title></root>\"},\"io.tapdata.entity.schema.value.TapYearValue\":{\"value\":{\"containsIllegal\":false,\"fraction\":3,\"nano\":0,\"originType\":90,\"seconds\":-62135798400}},\"java.lang.String\":\"中文\",\"[B\":\"5Lit5paH\"}"
                    , "gAFkABdqYXZhLnV0aWwuTGlua2VkSGFzaE1hcAEUAARudWxsAAEUABFqYXZhLmxhbmcuSW50ZWdlcgEVAAAAAQEUAA9qYXZhLmxhbmcuRmxvYXQBGD+MzM0BFAAQamF2YS5sYW5nLkRvdWJsZQEXP/GZmZmZmZoBFAAUamF2YS5tYXRoLkJpZ0RlY2ltYWwBGgADMS4xARQADmphdmEubGFuZy5Mb25nARkAAAGam2VndwEUABFqYXZhLnRpbWUuSW5zdGFudAEgAAAAAGkdipkbo3hQARQAEmphdmEuc3FsLlRpbWVzdGFtcAEfAAABmptlZ3cBFAAOamF2YS51dGlsLkRhdGUBHgAAAZqbZWd3ARQADWphdmEuc3FsLkRhdGUBHgAAAZqbZWd3ARQALGlvLnRhcGRhdGEuZW50aXR5LnNjaGVtYS52YWx1ZS5UYXBBcnJheVZhbHVlAWYALGlvLnRhcGRhdGEuZW50aXR5LnNjaGVtYS52YWx1ZS5UYXBBcnJheVZhbHVlARQAC29yaWdpblZhbHVlAAEUAApvcmlnaW5UeXBlAAEUAAV2YWx1ZQFlABNqYXZhLnV0aWwuQXJyYXlMaXN0ARQAATEBFAABMqgBFAAHdGFwVHlwZQCoARQALWlvLnRhcGRhdGEuZW50aXR5LnNjaGVtYS52YWx1ZS5UYXBCaW5hcnlWYWx1ZQFmAC1pby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwQmluYXJ5VmFsdWUBFAALb3JpZ2luVmFsdWUAARQACm9yaWdpblR5cGUAARQABXZhbHVlAQQAJ2lvLnRhcGRhdGEuZW50aXR5LnNjaGVtYS52YWx1ZS5CeXRlRGF0YQEAAAAABuS4reaWhwEUAAd0YXBUeXBlAKgBFAAuaW8udGFwZGF0YS5lbnRpdHkuc2NoZW1hLnZhbHVlLlRhcEJvb2xlYW5WYWx1ZQFmAC5pby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwQm9vbGVhblZhbHVlARQAC29yaWdpblZhbHVlAAEUAApvcmlnaW5UeXBlAAEUAAV2YWx1ZQEBrO0ABXNyABFqYXZhLmxhbmcuQm9vbGVhbs0gcoDVnPruAgABWgAFdmFsdWV4cAEBFAAHdGFwVHlwZQCoARQAL2lvLnRhcGRhdGEuZW50aXR5LnNjaGVtYS52YWx1ZS5UYXBEYXRlVGltZVZhbHVlAWYAL2lvLnRhcGRhdGEuZW50aXR5LnNjaGVtYS52YWx1ZS5UYXBEYXRlVGltZVZhbHVlARQAC29yaWdpblZhbHVlAAEUAApvcmlnaW5UeXBlAAEUAAV2YWx1ZQEEACdpby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuRGF0ZVRpbWUAAABaAAAAAwEAAAAAZ3QVAAEAAAAAAQAAAAEUAAd0YXBUeXBlAKgBFAAraW8udGFwZGF0YS5lbnRpdHkuc2NoZW1hLnZhbHVlLlRhcERhdGVWYWx1ZQFmACtpby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwRGF0ZVZhbHVlARQAC29yaWdpblZhbHVlAAEUAApvcmlnaW5UeXBlAAEUAAV2YWx1ZQEEACdpby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuRGF0ZVRpbWUAAABaAAAAAwEAAAAAZ3QVAAEAAAAAAQAAAAEUAAd0YXBUeXBlAKgBFAAraW8udGFwZGF0YS5lbnRpdHkuc2NoZW1hLnZhbHVlLlRhcEpzb25WYWx1ZQFmACtpby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwSnNvblZhbHVlARQAC29yaWdpblZhbHVlAAEUAApvcmlnaW5UeXBlAAEUAAV2YWx1ZQEUABl7ImlkIjoxLCJ0aXRsZSI6IuS4reaWhyJ9ARQAB3RhcFR5cGUAqAEUACppby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwTWFwVmFsdWUBZgAqaW8udGFwZGF0YS5lbnRpdHkuc2NoZW1hLnZhbHVlLlRhcE1hcFZhbHVlARQAC29yaWdpblZhbHVlAAEUAApvcmlnaW5UeXBlAAEUAAV2YWx1ZQFkABdqYXZhLnV0aWwuTGlua2VkSGFzaE1hcAEUAAJpZAEVAAAAAQEUAAV0aXRsZQEUAAbkuK3mloeoARQAB3RhcFR5cGUAqAEUACxpby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwTW9uZXlWYWx1ZQFmACxpby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwTW9uZXlWYWx1ZQEUAAtvcmlnaW5WYWx1ZQABFAAKb3JpZ2luVHlwZQABFAAFdmFsdWUBFz/xmZmZmZmaARQAB3RhcFR5cGUAqAEUAC1pby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwTnVtYmVyVmFsdWUBZgAtaW8udGFwZGF0YS5lbnRpdHkuc2NoZW1hLnZhbHVlLlRhcE51bWJlclZhbHVlARQAC29yaWdpblZhbHVlAAEUAApvcmlnaW5UeXBlAAEUAAV2YWx1ZQEXP/GZmZmZmZoBFAAHdGFwVHlwZQCoARQAKmlvLnRhcGRhdGEuZW50aXR5LnNjaGVtYS52YWx1ZS5UYXBSYXdWYWx1ZQFmACppby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwUmF3VmFsdWUBFAALb3JpZ2luVmFsdWUAARQACm9yaWdpblR5cGUAARQABXZhbHVlARYAAAAG5Lit5paHARQAB3RhcFR5cGUAqAEUAC1pby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwU3RyaW5nVmFsdWUBZgAtaW8udGFwZGF0YS5lbnRpdHkuc2NoZW1hLnZhbHVlLlRhcFN0cmluZ1ZhbHVlARQAC29yaWdpblZhbHVlAAEUAApvcmlnaW5UeXBlAAEUAAV2YWx1ZQEUAAbkuK3mlocBFAAHdGFwVHlwZQCoARQAK2lvLnRhcGRhdGEuZW50aXR5LnNjaGVtYS52YWx1ZS5UYXBUaW1lVmFsdWUBZgAraW8udGFwZGF0YS5lbnRpdHkuc2NoZW1hLnZhbHVlLlRhcFRpbWVWYWx1ZQEUAAtvcmlnaW5WYWx1ZQABFAAKb3JpZ2luVHlwZQABFAAFdmFsdWUBBAAnaW8udGFwZGF0YS5lbnRpdHkuc2NoZW1hLnZhbHVlLkRhdGVUaW1lAAAAAQAAAAMBAAAAAAAAAAEBAAAAAAEAAAABFAAHdGFwVHlwZQCoARQAKmlvLnRhcGRhdGEuZW50aXR5LnNjaGVtYS52YWx1ZS5UYXBYbWxWYWx1ZQFmACppby50YXBkYXRhLmVudGl0eS5zY2hlbWEudmFsdWUuVGFwWG1sVmFsdWUBFAALb3JpZ2luVmFsdWUAARQACm9yaWdpblR5cGUAARQABXZhbHVlARQASTw/eG1sIHZlcnNpb249IjEuMCIgZW5jb2Rpbmc9IlVURi04Ij8+Cjxyb290Pjx0aXRsZT7kuK3mloc8L3RpdGxlPjwvcm9vdD4BFAAHdGFwVHlwZQCoARQAK2lvLnRhcGRhdGEuZW50aXR5LnNjaGVtYS52YWx1ZS5UYXBZZWFyVmFsdWUBZgAraW8udGFwZGF0YS5lbnRpdHkuc2NoZW1hLnZhbHVlLlRhcFllYXJWYWx1ZQEUAAtvcmlnaW5WYWx1ZQABFAAKb3JpZ2luVHlwZQABFAAFdmFsdWUBBAAnaW8udGFwZGF0YS5lbnRpdHkuc2NoZW1hLnZhbHVlLkRhdGVUaW1lAAAAWgAAAAMB////8Yhq9YABAAAAAAEAAAABFAAHdGFwVHlwZQCoARQAEGphdmEubGFuZy5TdHJpbmcBFAAG5Lit5paHARQAAltCARYAAAAG5Lit5paHqA=="
                )
            );

            for (VersionCheckItem o : checkVersionList) {
                String encodeJson = o.getEncodeJson();
                String encodeData = o.getEncodeData();

                // 对比 10 次，排除随机性
                for (int i = 0; i < 10; i++) {
                    Object decodeObj = TaskInspectUtils.keysDeserialization(encodeData, null);
                    String decodeJson = JSON.toJSONString(decodeObj);
                    assertTrue(encodeJson.equals(decodeJson), String.format("version incompatible '%s'\n- from: %s\n-   to: %s\n"
                        , o.getVersion()
                        , encodeJson
                        , decodeJson)
                    );
                }
            }
        }

        @Getter
        @Setter
        static class VersionCheckItem {
            private String version;
            private String encodeJson;
            private String encodeData;

            static VersionCheckItem of(String version, String encodeJson, String encodeData) {
                VersionCheckItem ins = new VersionCheckItem();
                ins.version = version;
                ins.encodeJson = encodeJson;
                ins.encodeData = encodeData;
                return ins;
            }
        }

        static class TypeCheckItem<T> {
            String type;
            T value;
            BiPredicate<TypeCheckItem<T>, Object> compare;

            static <T> TypeCheckItem<T> of(T value, BiPredicate<TypeCheckItem<T>, Object> compare) {
                TypeCheckItem<T> ins = new TypeCheckItem<>();
                if (null == value) {
                    ins.type = "null";
                } else {
                    ins.type = value.getClass().getName();
                }
                ins.value = value;
                ins.compare = compare;
                return ins;
            }
        }
    }
}
