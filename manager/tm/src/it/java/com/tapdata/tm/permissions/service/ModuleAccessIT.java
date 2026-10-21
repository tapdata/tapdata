package com.tapdata.tm.permissions.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.userLog.controller.UserLogController;
import com.tapdata.tm.userLog.service.UserLogService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** L2: real module access decisions, audit list gate and permission seed; permission/settings storage are boundaries. */
@Timeout(10)
class ModuleAccessIT {
    private static final String SEED_VERSION = "4.25-1";
    private static final String ADMIN_ROLE = "5b9a0a383fcba02649524bf1";
    private GenericApplicationContext context;
    private ModuleAccess access;
    private PermissionService permissions;
    private SettingsService settings;
    private UserDetail user;

    @BeforeEach
    void setUp() {
        permissions = mock(PermissionService.class);
        settings = mock(SettingsService.class);
        user = new UserDetail("u1", "customer", "u1", "unused", List.of());
        context = new GenericApplicationContext();
        context.registerBean(ModuleAccess.class, () -> new ModuleAccess(permissions, settings));
        context.refresh();
        access = context.getBean(ModuleAccess.class);
    }

    @AfterEach
    void tearDown() {
        user.setFreeAuth(false);
        context.close();
    }

    @Test
    @DisplayName("AC10 模块权限判定：权限点放行并可看全部；云版放行但不看全部；URL 白名单标记 freeAuth 不放行")
    void should_decide_by_permission_cloud_and_free_auth() {
        String perm = DataPermissionEnumsName.V2_ALARM_EVENT;
        assertThat(access.allowed(user, perm)).isFalse();
        assertThat(access.allData(user, perm)).isFalse();
        assertThatThrownBy(() -> access.require(user, perm))
                .isInstanceOfSatisfying(BizException.class, e -> assertThat(e.getErrorCode()).isEqualTo("NotAuthorized"));
        assertThat(access.allowed(null, perm)).isFalse();

        when(permissions.checkCurrentUserHasPermission(perm, "u1")).thenReturn(true);
        assertThat(access.allowed(user, perm)).isTrue();
        assertThat(access.allData(user, perm)).isTrue();
        assertThat(access.allowed(user, DataPermissionEnumsName.V2_ALARM_EVENT_ACK_CLOSE)).isFalse();

        when(settings.isCloud()).thenReturn(true);
        assertThat(access.allowed(user, DataPermissionEnumsName.V2_AUDIT_LOG)).isTrue();
        assertThat(access.allData(user, perm)).isFalse();

        // freeAuth 只标记命中 Agent/集群等 URL 白名单的请求，与模块权限无关
        when(settings.isCloud()).thenReturn(false);
        user.setFreeAuth(true);
        assertThat(access.allowed(user, DataPermissionEnumsName.V2_NOTIFICATION_RECORD)).isFalse();
        assertThat(access.allData(user, DataPermissionEnumsName.V2_NOTIFICATION_RECORD)).isFalse();
    }

    @Test
    @DisplayName("AC10 操作审计列表需要审计查看权限，无权限时不查询")
    void should_gate_audit_log_list() {
        UserLogService logs = mock(UserLogService.class);
        when(logs.find(any(Filter.class), eq(user))).thenReturn(new Page<>());
        UserLogController controller = spy(new UserLogController(logs, access));
        doReturn(user).when(controller).getLoginUser();

        assertThatThrownBy(() -> controller.list(null)).isInstanceOf(BizException.class);
        verify(logs, never()).find(any(Filter.class), any(UserDetail.class));

        when(permissions.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_AUDIT_LOG, "u1")).thenReturn(true);
        controller.list(null);
        verify(logs).find(any(Filter.class), eq(user));
    }

    @Test
    @DisplayName("AC10 权限种子：新增权限树、页面路径与前端路由一致，并授予内置 admin 角色")
    void should_seed_permissions_for_admin() throws IOException {
        assertThat(read("init/idaas/version").trim()).isEqualTo(SEED_VERSION);
        JsonNode seed = new ObjectMapper().readTree(read("init/idaas/" + SEED_VERSION + ".json"));

        Map<String, JsonNode> perms = new HashMap<>();
        Set<String> granted = new HashSet<>();
        for (JsonNode block : seed) {
            for (JsonNode update : block.get("updates")) {
                JsonNode set = update.get("u").get("$set");
                assertThat(update.get("upsert").asBoolean()).isTrue();
                if ("Permission".equals(block.get("update").asText())) {
                    assertThat(update.get("q").get("name").asText()).isEqualTo(set.get("name").asText());
                    perms.put(set.get("name").asText(), set);
                } else if ("RoleMapping".equals(block.get("update").asText())) {
                    assertThat(update.get("q").get("roleId").get("$oid").asText()).isEqualTo(ADMIN_ROLE);
                    assertThat(set.get("principalType").asText()).isEqualTo("PERMISSION");
                    granted.add(set.get("principalId").asText());
                }
            }
        }

        assertThat(perms.keySet()).containsExactlyInAnyOrder("v2_ops-center", "v2_alarm_event", "v2_alarm_event_ack_close",
                "v2_message-center", "v2_notification_record", "v2_audit_log", "v2_audit_log_list");
        assertThat(granted).isEqualTo(perms.keySet());
        assertThat(perms.keySet()).contains(DataPermissionEnumsName.V2_ALARM_EVENT, DataPermissionEnumsName.V2_ALARM_EVENT_ACK_CLOSE,
                DataPermissionEnumsName.V2_NOTIFICATION_RECORD, DataPermissionEnumsName.V2_AUDIT_LOG);
        assertParent(perms, "v2_alarm_event", "v2_ops-center");
        assertParent(perms, "v2_alarm_event_ack_close", "v2_alarm_event");
        assertParent(perms, "v2_notification_record", "v2_message-center");
        assertParent(perms, "v2_audit_log", "v2_system-management");
        assertParent(perms, "v2_audit_log_list", "v2_audit_log");
        assertPage(perms, "v2_alarm_event", "/alarm-events");
        assertPage(perms, "v2_notification_record", "/notification-records");
        assertPage(perms, "v2_audit_log_list", "/operation-audit");
        assertThat(perms.get("v2_alarm_event_ack_close").get("resources").get(0).get("type").asText()).isEqualTo("button");
    }

    private static void assertParent(Map<String, JsonNode> perms, String name, String parent) {
        assertThat(perms.get(name).get("parentId").asText()).isEqualTo(parent);
    }

    private static void assertPage(Map<String, JsonNode> perms, String name, String path) {
        JsonNode resource = perms.get(name).get("resources").get(0);
        assertThat(resource.get("type").asText()).isEqualTo("page");
        assertThat(resource.get("code").asText()).isEqualTo(name);
        assertThat(resource.get("path").asText()).isEqualTo(path);
    }

    private static String read(String path) throws IOException {
        try (InputStream in = new ClassPathResource(path).getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
