package com.tapdata.tm.group.service;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.group.repostitory.GroupInfoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * T7-12a · {@code GroupInfoService.injectVaultSecrets} 的 N+1 批量化。
 *
 * <p>它今天对<b>每条连接</b>调一次 {@code findByPdkHash}；同文件的 diff 路径早已改成
 * {@code findByPdkHashList} 批量查 + map 查找。两者对 {@code pdkHash == null} 的处理
 * <b>不同</b>（批量路径有 {@code != null} 过滤，逐条路径把 null 直接传进去）——
 * 这正是批量化改写最容易悄悄改掉的边角，故单独钉住。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GroupInfoServiceVaultInjectTest {

    @Mock
    private GroupInfoRepository groupInfoRepository;
    @Mock
    private DataSourceDefinitionService dataSourceDefinitionService;

    private GroupInfoService groupInfoService;
    private UserDetail user;

    @BeforeEach
    void setUp() {
        groupInfoService = new GroupInfoService(groupInfoRepository);
        ReflectionTestUtils.setField(groupInfoService, "dataSourceDefinitionService", dataSourceDefinitionService);
        user = new UserDetail("userId123", "customerId", "testuser", "password", "customerType",
                "accessCode", false, false, false, false,
                Collections.singletonList(new SimpleGrantedAuthority("role")));
    }

    private DataSourceConnectionDto conn(String name, String pdkHash) {
        DataSourceConnectionDto c = new DataSourceConnectionDto();
        c.setName(name);
        c.setPdkHash(pdkHash);
        c.setConfig(new LinkedHashMap<>());
        return c;
    }

    @Test
    @DisplayName("T7-12a 批量查 definition：一次 findByPdkHashList，null pdkHash 被过滤且该连接照常注入")
    void t7_12a_batchesDefinitionLookupAndToleratesNullPdkHash() {
        DataSourceConnectionDto a = conn("CONN_A", "hash-1");
        DataSourceConnectionDto b = conn("CONN_B", "hash-1"); // 同型，应共用一次查询
        DataSourceConnectionDto c = conn("CONN_C", null);     // 边角：没有 pdkHash

        Map<String, DataSourceConnectionDto> connections = new LinkedHashMap<>();
        connections.put("a", a);
        connections.put("b", b);
        connections.put("c", c);

        DataSourceDefinitionDto def = new DataSourceDefinitionDto();
        def.setPdkHash("hash-1");
        when(dataSourceDefinitionService.findByPdkHashList(any(), any())).thenReturn(List.of(def));

        Map<String, String> vault = new LinkedHashMap<>();
        vault.put("CONN_A_URL", "ha:5432");
        vault.put("CONN_A_USER", "ua");
        vault.put("CONN_A_PASSWORD", "pa");
        vault.put("CONN_B_URL", "hb:5432");
        vault.put("CONN_B_USER", "ub");
        vault.put("CONN_B_PASSWORD", "pb");
        vault.put("CONN_C_URL", "hc:5432");
        vault.put("CONN_C_USER", "uc");
        vault.put("CONN_C_PASSWORD", "pc");

        ReflectionTestUtils.invokeMethod(groupInfoService, "injectVaultSecrets", connections, vault, user);

        // 批量：一次查询覆盖全部连接
        ArgumentCaptor<Set<String>> captor = ArgumentCaptor.forClass(Set.class);
        verify(dataSourceDefinitionService, times(1)).findByPdkHashList(captor.capture(), any());
        assertEquals(Set.of("hash-1"), captor.getValue(), "null pdkHash 必须被过滤掉，不能进查询集合");

        // 逐条查询必须彻底消失，否则 N+1 还在
        verify(dataSourceDefinitionService, never()).findByPdkHash(anyString(), anyInt(), any());

        // pdkHash == null 的那条不抛异常、照常走无 definition 的分支（fallback config key）
        assertEquals("pc", c.getConfig().get("password"));
        assertEquals("uc", c.getConfig().get("username"));
        assertEquals("hc", c.getConfig().get("host"));
        assertFalse(a.getConfig().isEmpty(), "有 definition 的连接同样照常注入");
    }
}
