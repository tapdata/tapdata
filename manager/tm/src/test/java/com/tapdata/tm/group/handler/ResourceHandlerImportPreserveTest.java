package com.tapdata.tm.group.handler;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 导入侧：包里缺敏感字段时，绝不用空值覆盖目标环境已有的配置（ADR-0034 D5/D6/D7）。
 *
 * 反面教材就是这条链路今天的行为：导出无条件脱敏 → 包里 uri 被抹空 → GROUP_IMPORT 走
 * handleGroupImportConnection → importSave 整文档覆盖 → 目标连接的 uri 变成空，
 * 导入还报成功。实撞记录：`Create MongoDB client failed, error: uri is blank`。
 */
@DisplayName("ResourceHandler import-side secret preservation")
public class ResourceHandlerImportPreserveTest {

    /** definition.properties.connection.properties.{uri -> apiServerKey=database_uri} */
    private DataSourceDefinitionDto definitionWithUri() {
        Map<String, Object> uriMeta = new LinkedHashMap<>();
        uriMeta.put("apiServerKey", "database_uri");
        Map<String, Object> connProps = new LinkedHashMap<>();
        connProps.put("uri", uriMeta);
        Map<String, Object> connection = new LinkedHashMap<>();
        connection.put("properties", connProps);
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        properties.put("connection", connection);

        DataSourceDefinitionDto definition = new DataSourceDefinitionDto();
        definition.setProperties(properties);
        return definition;
    }

    private DataSourceConnectionDto connection(Map<String, Object> config) {
        DataSourceConnectionDto conn = new DataSourceConnectionDto();
        conn.setName("MDM_CUSTOMER");
        conn.setConfig(config);
        return conn;
    }

    @Test
    @DisplayName("包里 uri 缺失时保留目标既有 uri，并报告该字段")
    void missingSensitiveField_keepsExistingValueAndReportsIt() {
        DataSourceConnectionDto incoming = connection(new LinkedHashMap<>(Map.of("isUri", true)));
        Map<String, Object> existingConfig = new LinkedHashMap<>(Map.of(
                "isUri", true,
                "uri", "mongodb://real-host:27017/dmp"));

        List<String> preserved = ResourceHandler.restoreMissingSecretsFromExisting(
                incoming, existingConfig, definitionWithUri());

        assertEquals("mongodb://real-host:27017/dmp", incoming.getConfig().get("uri"),
                "目标既有 uri 必须被保留——包里那个空缺是脱敏的产物，不是用户的配置");
        assertEquals(List.of("uri"), preserved,
                "被保留的字段必须报出来，否则就是静默（ADR-0034 D7）");
    }
}
