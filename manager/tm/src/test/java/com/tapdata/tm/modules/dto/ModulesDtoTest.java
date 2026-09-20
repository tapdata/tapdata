package com.tapdata.tm.modules.dto;


import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.PathSetting;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ModulesDtoTest {

    /**
     * TAP-12425：项目导出/导入用 {@link JsonUtil} 序列化 ModulesDto，connection 必须原样往返。
     * 曾因该字段没有 ObjectId 序列化器，被 Jackson 当普通 bean 写成 {"timestamp":..,"date":..}，
     * 回读时构造出一个全新的随机 ObjectId，导致导入后的 API 指向一个不存在的连接。
     */
    @Nested
    @DisplayName("connection 字段 JSON 往返（TAP-12425）")
    class ConnectionJsonRoundTripTest {

        @Test
        @DisplayName("导出再导入后 connection 保持不变")
        void testConnectionSurvivesJsonRoundTrip() {
            ObjectId connectionId = new ObjectId("68a1b2c3d4e5f60718293a4b");
            ModulesDto dto = new ModulesDto();
            dto.setId(new ObjectId("6a68995344ec8e331a7f4119"));
            dto.setName("cpi_case");
            dto.setConnection(connectionId);
            dto.setConnectionId(connectionId.toHexString());
            dto.setDataSource(connectionId.toHexString());

            String json = JsonUtil.toJsonUseJackson(dto);
            ModulesDto parsed = JsonUtil.parseJsonUseJackson(json, ModulesDto.class);

            Assertions.assertNotNull(parsed);
            Assertions.assertEquals(connectionId, parsed.getConnection(),
                    "connection 在 JSON 往返后被改写，导入的 API 将指向不存在的连接：" + json);
        }

        @Test
        @DisplayName("connection 序列化为 24 位 hex 字符串，而非 {timestamp,date} 对象")
        void testConnectionSerializedAsHexString() {
            ObjectId connectionId = new ObjectId("68a1b2c3d4e5f60718293a4b");
            ModulesDto dto = new ModulesDto();
            dto.setConnection(connectionId);

            String json = JsonUtil.toJsonUseJackson(dto);

            Assertions.assertTrue(json.contains("\"connection\":\"68a1b2c3d4e5f60718293a4b\""),
                    "connection 应序列化为 hex 字符串，实际为：" + json);
        }

        @Test
        @DisplayName("老导出包的 {timestamp,date} 形态解析为 null，而不是随机 ObjectId")
        void testLegacyObjectShapeParsesToNullInsteadOfRandomId() {
            // 修复前生成的导出包长这样：connection 被当普通 bean 序列化，原始 id 已不可恢复。
            // 此时必须给出 null（由导入侧按 datasource 重建），而不是编一个能通过校验的假 id。
            String legacyJson = "{\"id\":\"6a68995344ec8e331a7f4119\",\"name\":\"cpi_case\","
                    + "\"connectionId\":\"68a1b2c3d4e5f60718293a4b\","
                    + "\"connection\":{\"timestamp\":1755427523,\"date\":1755427523000},"
                    + "\"datasource\":\"68a1b2c3d4e5f60718293a4b\"}";

            ModulesDto parsed = JsonUtil.parseJsonUseJackson(legacyJson, ModulesDto.class);

            Assertions.assertNotNull(parsed);
            Assertions.assertNull(parsed.getConnection());
            Assertions.assertEquals("68a1b2c3d4e5f60718293a4b", parsed.getDataSource());
            Assertions.assertEquals("68a1b2c3d4e5f60718293a4b", parsed.getConnectionId());
        }

        @Test
        @DisplayName("connection 为 null 时不落字段，回读同样为 null")
        void testNullConnection() {
            ModulesDto dto = new ModulesDto();
            dto.setName("no_connection");

            String json = JsonUtil.toJsonUseJackson(dto);
            ModulesDto parsed = JsonUtil.parseJsonUseJackson(json, ModulesDto.class);

            Assertions.assertNotNull(parsed);
            Assertions.assertNull(parsed.getConnection());
        }
    }

    @Nested
    class WithPathSettingIfNeedTest {
        @Test
        void testWithPathSettingIfNeed() {
            ModulesDto dto = new ModulesDto();
            dto.withPathSettingIfNeed();
            Assertions.assertNotNull(dto.getPathSetting());
            Assertions.assertEquals(PathSetting.DEFAULT_PATH_SETTING, dto.getPathSetting());

            ModulesDto dto1 = new ModulesDto();
            dto1.setPathSetting(PathSetting.DEFAULT_PATH_SETTING);
            dto1.withPathSettingIfNeed();
            Assertions.assertNotNull(dto.getPathSetting());
            Assertions.assertEquals(PathSetting.DEFAULT_PATH_SETTING, dto.getPathSetting());
        }
    }
}