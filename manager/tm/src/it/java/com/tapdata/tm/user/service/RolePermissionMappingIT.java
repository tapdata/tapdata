package com.tapdata.tm.user.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.Permission.entity.PermissionEntity;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.user.dto.UpdatePermissionRoleMappingDto;
import com.tapdata.tm.user.repository.UserRepository;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * L2: real {@link UserServiceImpl#updatePermissionRoleMapping} linkage over the 4.25-1 permission seed;
 * permission and role-mapping storage are in-memory boundaries asserted out of band.
 */
@Timeout(10)
class RolePermissionMappingIT {
    private static final String SEED_VERSION = "4.25-1";
    private static final String OPS_CENTER = "v2_ops-center";
    private static final String ALARM_EVENT = "v2_alarm_event";
    private static final String ALARM_CLOSE = "v2_alarm_event_ack_close";

    private GenericApplicationContext context;
    private UserServiceImpl service;
    private final List<PermissionEntity> permissions = new ArrayList<>();
    private final List<RoleMappingDto> mappings = new ArrayList<>();
    private final ObjectId roleId = new ObjectId();
    private final UserDetail admin = new UserDetail("admin", "customer", "admin", "unused", List.of());

    @BeforeEach
    void setUp() throws IOException {
        loadSeed();
        PermissionService permissionService = mock(PermissionService.class);
        when(permissionService.find(any(Filter.class))).thenAnswer(i -> {
            Map<String, Object> where = i.<Filter>getArgument(0).getWhere();
            return permissions.stream().filter(p -> matches(where, field(p))).collect(Collectors.toList());
        });
        RoleMappingService roleMappingService = mock(RoleMappingService.class);
        when(roleMappingService.findAll(any(Query.class))).thenAnswer(i -> mappingsMatching(i.getArgument(0)));
        when(roleMappingService.count(any(Query.class))).thenAnswer(i -> (long) mappingsMatching(i.getArgument(0)).size());
        when(roleMappingService.deleteAll(any(Query.class))).thenAnswer(i -> {
            List<RoleMappingDto> removed = mappingsMatching(i.getArgument(0));
            mappings.removeAll(removed);
            return (long) removed.size();
        });
        when(roleMappingService.save(anyList(), any(UserDetail.class))).thenAnswer(i -> {
            List<RoleMappingDto> saved = i.getArgument(0);
            mappings.addAll(saved);
            return saved;
        });

        context = new GenericApplicationContext();
        context.registerBean(UserServiceImpl.class, () -> {
            UserServiceImpl impl = new UserServiceImpl(mock(UserRepository.class));
            ReflectionTestUtils.setField(impl, "permissionService", permissionService);
            ReflectionTestUtils.setField(impl, "roleMappingService", roleMappingService);
            return impl;
        });
        context.refresh();
        service = context.getBean(UserServiceImpl.class);
    }

    @AfterEach
    void tearDown() {
        context.close();
        permissions.clear();
        mappings.clear();
    }

    @Test
    @DisplayName("TAP-13349 仅勾选告警事件查看保存：只授予告警事件及运维中心菜单，不自动授予告警关闭权限")
    void should_not_grant_close_when_only_alarm_event_added() {
        service.updatePermissionRoleMapping(update(List.of(ALARM_EVENT), List.of()), admin);

        assertThat(granted()).containsExactlyInAnyOrder(ALARM_EVENT, OPS_CENTER);
    }

    @Test
    @DisplayName("TAP-13349 单独取消告警关闭权限：只删除关闭权限，告警事件查看与运维中心菜单保留")
    void should_keep_alarm_event_when_only_close_removed() {
        grant(OPS_CENTER, ALARM_EVENT, ALARM_CLOSE);

        service.updatePermissionRoleMapping(update(List.of(), List.of(ALARM_CLOSE)), admin);

        assertThat(granted()).containsExactlyInAnyOrder(ALARM_EVENT, OPS_CENTER);
    }

    @Test
    @DisplayName("TAP-13349 取消告警事件页面权限：连带删除其关闭权限，运维中心下无剩余页面时菜单一并删除")
    void should_remove_close_and_menu_when_alarm_event_removed() {
        grant(OPS_CENTER, ALARM_EVENT, ALARM_CLOSE);

        service.updatePermissionRoleMapping(update(List.of(), List.of(ALARM_EVENT)), admin);

        assertThat(granted()).isEmpty();
    }

    private UpdatePermissionRoleMappingDto update(List<String> adds, List<String> deletes) {
        UpdatePermissionRoleMappingDto dto = new UpdatePermissionRoleMappingDto();
        dto.setAdds(adds.stream().map(this::mapping).collect(Collectors.toList()));
        dto.setDeletes(deletes.stream().map(this::mapping).collect(Collectors.toList()));
        return dto;
    }

    private RoleMappingDto mapping(String code) {
        return new RoleMappingDto(PrincipleType.PERMISSION.getValue(), code, roleId);
    }

    private void grant(String... codes) {
        for (String code : codes) {
            mappings.add(mapping(code));
        }
    }

    private Set<String> granted() {
        return mappings.stream().filter(m -> roleId.equals(m.getRoleId()))
                .map(RoleMappingDto::getPrincipalId).collect(Collectors.toSet());
    }

    private void loadSeed() throws IOException {
        JsonNode seed;
        try (InputStream in = new ClassPathResource("init/idaas/" + SEED_VERSION + ".json").getInputStream()) {
            seed = new ObjectMapper().readTree(in);
        }
        for (JsonNode block : seed) {
            if (!"Permission".equals(block.path("update").asText())) {
                continue;
            }
            for (JsonNode update : block.get("updates")) {
                JsonNode set = update.get("u").get("$set");
                PermissionEntity entity = new PermissionEntity();
                entity.setName(set.get("name").asText());
                entity.setParentId(set.get("parentId").asText());
                permissions.add(entity);
            }
        }
        assertThat(permissions).extracting(PermissionEntity::getName).contains(OPS_CENTER, ALARM_EVENT, ALARM_CLOSE);
    }

    private List<RoleMappingDto> mappingsMatching(Query query) {
        Document filter = query.getQueryObject();
        return mappings.stream().filter(m -> matches(filter, key -> switch (key) {
            case "roleId" -> m.getRoleId();
            case "principalId" -> m.getPrincipalId();
            case "principalType" -> m.getPrincipalType();
            default -> throw new IllegalArgumentException("unexpected role mapping field " + key);
        })).collect(Collectors.toList());
    }

    private static java.util.function.Function<String, Object> field(PermissionEntity p) {
        return key -> switch (key) {
            case "name" -> p.getName();
            case "parentId" -> p.getParentId();
            default -> throw new IllegalArgumentException("unexpected permission field " + key);
        };
    }

    /** Minimal Mongo filter evaluation for the operators used by the role mapping code path. */
    @SuppressWarnings("unchecked")
    private static boolean matches(Map<String, Object> filter, java.util.function.Function<String, Object> doc) {
        for (Map.Entry<String, Object> e : filter.entrySet()) {
            String key = e.getKey();
            Object cond = e.getValue();
            boolean ok;
            if ("$or".equals(key)) {
                ok = ((Collection<Map<String, Object>>) cond).stream().anyMatch(c -> matches(c, doc));
            } else if ("$and".equals(key)) {
                ok = ((Collection<Map<String, Object>>) cond).stream().allMatch(c -> matches(c, doc));
            } else if (cond instanceof Map<?, ?> ops && ops.keySet().stream().allMatch(k -> String.valueOf(k).startsWith("$"))) {
                Object value = doc.apply(key);
                ok = ((Map<String, Object>) ops).entrySet().stream().allMatch(op -> operator(op.getKey(), op.getValue()).test(value));
            } else {
                ok = same(doc.apply(key), cond);
            }
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    private static Predicate<Object> operator(String op, Object arg) {
        return switch (op) {
            case "$in" -> v -> ((Collection<?>) arg).stream().anyMatch(a -> same(v, a));
            case "$ne" -> v -> !same(v, arg);
            case "$exists" -> v -> Boolean.TRUE.equals(arg) == (v != null);
            default -> throw new IllegalArgumentException("unsupported operator " + op);
        };
    }

    private static boolean same(Object actual, Object expected) {
        Object a = actual instanceof Enum<?> en ? en.name() : actual;
        Object b = expected instanceof Enum<?> en ? en.name() : expected;
        return Objects.equals(a, b);
    }
}
