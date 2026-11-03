package com.tapdata.tm.group.handler;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * ES-2：导出脱敏开关是否真的透到 payload（[ADR-0034] D1/D2）。
 *
 * 光有「策略解析对了」不够——ES-4 刚撞过「方法有测试、接线没有」。这里钉的是最后一段：
 * 开关为「保真」时包里必须留有真实凭据，为「脱敏」时必须抹掉。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("buildConnectionPayload 的脱敏开关")
public class ResourceHandlerExportMaskTest {

    private static final String REAL_URI = "mongodb://real-host:27017/dmp";

    @Mock
    private UserDetail user;

    private DataSourceDefinitionDto definitionWithUri() {
        Map<String, Object> uriMeta = new LinkedHashMap<>();
        uriMeta.put("apiServerKey", "database_uri");
        Map<String, Object> connProps = new LinkedHashMap<>();
        connProps.put("uri", uriMeta);
        Map<String, Object> connection = new LinkedHashMap<>();
        connection.put("properties", connProps);
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        properties.put("connection", connection);
        DataSourceDefinitionDto def = new DataSourceDefinitionDto();
        def.setProperties(properties);
        return def;
    }

    private DataSourceEntity connectionWithRealUri() {
        DataSourceEntity entity = new DataSourceEntity();
        entity.setId(new ObjectId());
        entity.setName("MDM_CUSTOMER");
        entity.setPdkHash("pdkhash-mongodb");
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("isUri", true);
        config.put("uri", REAL_URI);
        entity.setConfig(config);
        return entity;
    }

    /** 跑一次导出 payload 构建，返回连接文档的 JSON */
    private String exportedConnectionJson(DataSourceEntity entity, boolean maskSecrets) {
        DataSourceDefinitionService definitionService = mock(DataSourceDefinitionService.class);
        when(definitionService.findByPdkHash(any(), anyInt(), any())).thenReturn(definitionWithUri());
        MetadataInstancesService metadataInstancesService = mock(MetadataInstancesService.class);
        ResourceHandler handler = mock(ResourceHandler.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));

        try (MockedStatic<SpringUtil> springUtil = mockStatic(SpringUtil.class)) {
            springUtil.when(() -> SpringUtil.getBean(DataSourceDefinitionService.class)).thenReturn(definitionService);
            springUtil.when(() -> SpringUtil.getBean(MetadataInstancesService.class)).thenReturn(metadataInstancesService);

            List<TaskUpAndLoadDto> payload = handler.buildConnectionPayload(List.of(entity), user, maskSecrets);
            assertEquals(1, payload.size(), "一条连接应当产出一份连接文档（本例无元数据）");
            return payload.get(0).getJson();
        }
    }

    @Test
    @DisplayName("保真：包里留着真实 uri —— 本地导出的包要能直接用")
    void keepsSecretsWhenMaskDisabled() {
        String json = exportedConnectionJson(connectionWithRealUri(), false);
        assertTrue(json.contains(REAL_URI),
                "本地导出默认保真（ADR-0034 D1），凭据被抹掉的包搬到另一个环境就是不可用的：" + json);
    }

    @Test
    @DisplayName("脱敏：包里没有 uri")
    void masksSecretsWhenMaskEnabled() {
        String json = exportedConnectionJson(connectionWithRealUri(), true);
        assertFalse(json.contains(REAL_URI), "要求脱敏时凭据不得出现在包里：" + json);
    }

    @Test
    @DisplayName("脱敏不改动调用方传进来的其它 config 项")
    void masksOnlySensitivePaths() {
        DataSourceEntity entity = connectionWithRealUri();
        String json = exportedConnectionJson(entity, true);
        assertTrue(json.contains("isUri"), "只抹敏感路径，非敏感项照常导出：" + json);
    }
}
