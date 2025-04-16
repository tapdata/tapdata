package com.tapdata.taskinspect.vo;

import com.tapdata.constant.MD5Util;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockStatic;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/16 08:54 Create
 */
@ExtendWith(MockitoExtension.class)
class TaskInspectCdcEventTest {

    TaskInspectCdcEvent event;

    @BeforeEach
    void setUp() {
        event = new TaskInspectCdcEvent();
    }

    @Nested
    class putKeyTest {

        @Test
        void testPutKey_InitialKeys() {
            // 逻辑预设：keys 初始为 null
            String key = "id";
            Object value = 1;

            // 操作：调用 putKey 方法
            event.putKey(key, value);

            // 期望：keys 不为 null，且包含 key 和 value
            assertNotNull(event.getKeys());
            assertEquals(1, event.getKeys().size());
            assertEquals(value, event.getKeys().get(key));
        }

        @Test
        void testPutKey_ExistingKeys() {
            // 逻辑预设：keys 已经存在
            LinkedHashMap<String, Object> keys = new LinkedHashMap<>();
            keys.put("id", 1);
            event.setKeys(keys);

            String key = "name";
            Object value = "test";

            // 操作：调用 putKey 方法
            event.putKey(key, value);

            // 期望：keys 包含新的 key 和 value
            assertEquals(2, event.getKeys().size());
            assertEquals(1, event.getKeys().get("id"));
            assertEquals(value, event.getKeys().get(key));
        }
    }

    @Nested
    class initRowIdTest {

        @Test
        void testInitRowId_WithKeys() {
            // 逻辑预设：keys 存在
            String tableName = "table1";
            LinkedHashMap<String, Object> keys = new LinkedHashMap<>();
            keys.put("id", 1);
            event.setTableName(tableName);
            event.setKeys(keys);

            try(MockedStatic<MD5Util> md5UtilMockedStatic = mockStatic(MD5Util.class)) {
                // 模拟 MD5Util.crypt 方法
                String expectedRowId = "expectedRowId";
                md5UtilMockedStatic.when(()->MD5Util.crypt(tableName + "|1", false)).thenReturn(expectedRowId);

                // 操作：调用 initRowId 方法
                String rowId = event.initRowId();

                // 期望：rowId 被正确设置
                assertEquals(expectedRowId, rowId);
                assertEquals(expectedRowId, event.getRowId());
            }
        }
    }

    @Nested
    class createTest {

        @Nested
        class create_Time_TableName_Test {

            @Test
            void testCreate_Time_TableName() {
                // 逻辑预设：使用当前系统时间作为参考时间
                Long time = System.currentTimeMillis();
                String tableName = "table1";

                // 操作：调用 create 方法
                TaskInspectCdcEvent event = TaskInspectCdcEvent.create(time, tableName);

                // 期望：事件的 referenceTime 和 time 被正确设置
                assertNotNull(event.getReferenceTime());
                assertEquals(time, event.getTime());
                assertEquals(tableName, event.getTableName());
                assertNotNull(event.getKeys());
                assertTrue(event.getKeys().isEmpty());
            }
        }

        @Nested
        class create_ReferenceTime_Time_TableName_Test {

            @Test
            void testCreate_ReferenceTime_Time_TableName() {
                // 逻辑预设：使用指定的 referenceTime 和 time
                Long referenceTime = System.currentTimeMillis();
                Long time = referenceTime + 1000;
                String tableName = "table1";

                // 操作：调用 create 方法
                TaskInspectCdcEvent event = TaskInspectCdcEvent.create(referenceTime, time, tableName);

                // 期望：事件的 referenceTime 和 time 被正确设置
                assertEquals(referenceTime, event.getReferenceTime());
                assertEquals(time, event.getTime());
                assertEquals(tableName, event.getTableName());
                assertNotNull(event.getKeys());
                assertTrue(event.getKeys().isEmpty());
            }
        }

        @Nested
        class create_ReferenceTime_Time_TableName_Keys_Test {

            @Test
            void testCreate_ReferenceTime_Time_TableName_Keys() {
                // 逻辑预设：使用指定的 referenceTime, time, tableName 和 keys
                Long referenceTime = System.currentTimeMillis();
                Long time = referenceTime + 1000;
                String tableName = "table1";
                LinkedHashMap<String, Object> keys = new LinkedHashMap<>();
                keys.put("id", 1);

                // 操作：调用 create 方法
                TaskInspectCdcEvent event = TaskInspectCdcEvent.create(referenceTime, time, tableName, keys);

                // 期望：事件的 referenceTime, time, tableName 和 keys 被正确设置
                assertEquals(referenceTime, event.getReferenceTime());
                assertEquals(time, event.getTime());
                assertEquals(tableName, event.getTableName());
                assertNotNull(event.getKeys());
                assertEquals(1, event.getKeys().size());
                assertEquals(1, event.getKeys().get("id"));
            }

            @Test
            void testCreate_ReferenceTime_Time_TableName_NullKeys() {
                // 逻辑预设：使用指定的 referenceTime, time, tableName 和 null keys
                Long referenceTime = System.currentTimeMillis();
                Long time = referenceTime + 1000;
                String tableName = "table1";

                // 操作：调用 create 方法
                TaskInspectCdcEvent event = TaskInspectCdcEvent.create(referenceTime, time, tableName, null);

                // 期望：事件的 referenceTime, time, tableName 和 keys 被正确设置
                assertEquals(referenceTime, event.getReferenceTime());
                assertEquals(time, event.getTime());
                assertEquals(tableName, event.getTableName());
                assertNotNull(event.getKeys());
                assertTrue(event.getKeys().isEmpty());
            }
        }
    }
}
