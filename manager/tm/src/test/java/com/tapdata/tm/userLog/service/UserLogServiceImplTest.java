package com.tapdata.tm.userLog.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.constant.SyncType;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.constant.UserLogTemplateKey;
import com.tapdata.tm.userLog.constant.UserLogType;
import com.tapdata.tm.userLog.dto.UserLogDto;
import com.tapdata.tm.userLog.repository.UserLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class UserLogServiceImplTest {
    private UserLogServiceImpl userLogService;
    @BeforeEach
    void beforeEach() {
        userLogService = mock(UserLogServiceImpl.class);
    }
    @Nested
    class testAddUserLogWithSystemStart {
        @Test
        void testBoolean() {
            UserDetail userDetail = mock(UserDetail.class);
            doCallRealMethod().when(userLogService).addUserLog(Modular.USER, Operation.CREATE, userDetail, null, "", true);
            userLogService.addUserLog(Modular.USER, Operation.CREATE, userDetail, null, "", true);
            verify(userLogService, times(1)).addUserLog(Modular.USER, Operation.CREATE, userDetail, null, UserLogType.USER_OPERATION, "", null, false, true);
        }
        @Test
        void testNotNull() {
            UserDetail userDetail = mock(UserDetail.class);
            doCallRealMethod().when(userLogService).addUserLog(Modular.USER, Operation.CREATE, userDetail, null, "", "");
            userLogService.addUserLog(Modular.USER, Operation.CREATE, userDetail, null, "", "");
            verify(userLogService, times(1)).addUserLog(Modular.USER, Operation.CREATE, userDetail, null, UserLogType.USER_OPERATION, "", null, false, true);
        }
        @Test
        void testIsNull() {
            UserDetail userDetail = mock(UserDetail.class);
            doCallRealMethod().when(userLogService).addUserLog(Modular.USER, Operation.CREATE, userDetail, null, "", null);
            userLogService.addUserLog(Modular.USER, Operation.CREATE, userDetail, null, "", null);
            verify(userLogService, times(1)).addUserLog(Modular.USER, Operation.CREATE, userDetail, null, UserLogType.USER_OPERATION, "", null, false, false);
        }
    }

    @Nested
    class RenderI18nMessage {
        @BeforeEach
        void beforeEach() {
            userLogService = new UserLogServiceImpl(mock(UserLogRepository.class));
        }

        @Test
        void testDefaultTemplateWithModuleName() {
            UserLogDto dto = new UserLogDto();
            dto.setUsername("alice");
            dto.setModular("connection");
            dto.setOperation("create");
            dto.setParameter1("mysql_conn");

            String message = userLogService.renderI18nMessage(dto, Locale.CHINA);

            assertEquals("用户 {user} 创建了 连接 {parameter1}", message);
        }

        @Test
        void testDefaultTemplateWithMissingModuleName() {
            UserLogDto dto = new UserLogDto();
            dto.setUsername("alice");
            dto.setModular("unknown");
            dto.setOperation("create");
            dto.setParameter1("source");

            String message = userLogService.renderI18nMessage(dto, Locale.CHINA);

            assertEquals("用户 {user} 创建了 unknown {parameter1}", message);
        }

        @Test
        void testFallbackWhenSpecificAndDefaultTemplateMissing() {
            UserLogDto dto = new UserLogDto();
            dto.setUsername("alice");
            dto.setModular("sync");
            dto.setOperation("not_exists");
            dto.setParameter1("order_sync");

            String message = userLogService.renderI18nMessage(dto, Locale.CHINA);

            assertEquals("alice sync.not_exists order_sync", message);
        }

        @Test
        void testEnglishTemplate() {
            UserLogDto dto = new UserLogDto();
            dto.setUsername("alice");
            dto.setModular("sync");
            dto.setOperation("restart");
            dto.setParameter1("order_sync");

            String message = userLogService.renderI18nMessage(dto, Locale.US);

            assertEquals("User {user} restarted Sync Task {parameter1}", message);
        }

        @Test
        void testRestartUsesDefaultTemplateWithModuleName() {
            UserLogDto dto = new UserLogDto();
            dto.setUsername("alice");
            dto.setModular("migration");
            dto.setOperation("restart");
            dto.setParameter1("order_migration");

            String message = userLogService.renderI18nMessage(dto, Locale.US);

            assertEquals("User {user} restarted Migration Task {parameter1}", message);
        }

        @Test
        void testSpecificTemplateOverridesDefaultTemplate() {
            UserLogDto dto = new UserLogDto();
            dto.setUsername("alice");
            dto.setModular("mcp");
            dto.setOperation("connected");
            dto.setParameter1("server");

            String message = userLogService.renderI18nMessage(dto, Locale.US);

            assertEquals("User {user} connected MCP", message);
        }

        @Test
        void testReadAllTemplateWithModuleName() {
            UserLogDto dto = new UserLogDto();
            dto.setUsername("alice");
            dto.setModular("message");
            dto.setOperation("readAll");

            String message = userLogService.renderI18nMessage(dto, Locale.US);

            assertEquals("User {user} marked all items in Notification as read", message);
        }

        @Test
        void testDeleteAllTemplateWithModuleName() {
            UserLogDto dto = new UserLogDto();
            dto.setUsername("alice");
            dto.setModular("message");
            dto.setOperation("deleteAll");

            String message = userLogService.renderI18nMessage(dto, Locale.US);

            assertEquals("User {user} deleted all items in Notification", message);
        }
    }

    @Nested
    class UserLogsTemplateCoverage {
        private final List<String> templateFiles = Arrays.asList(
                "userLogsTemplate.properties",
                "userLogsTemplate_en_US.properties",
                "userLogsTemplate_zh_CN.properties",
                "userLogsTemplate_zh_TW.properties"
        );

        @Test
        void testEveryOperationHasTemplate() throws IOException {
            for (String templateFile : templateFiles) {
                Properties properties = loadTemplate(templateFile);
                List<String> missingKeys = new ArrayList<>();
                for (Operation operation : Operation.values()) {
                    String defaultKey = UserLogTemplateKey.defaultOperation(operation.getValue());
                    if (properties.containsKey(defaultKey) || hasSpecificOperationTemplate(properties, operation.getValue())) {
                        continue;
                    }
                    missingKeys.add(defaultKey);
                }

                assertTrue(missingKeys.isEmpty(), templateFile + " missing operation templates: " + missingKeys);
            }
        }

        @Test
        void testEveryModularHasModuleNameTemplate() throws IOException {
            for (String templateFile : templateFiles) {
                Properties properties = loadTemplate(templateFile);
                List<String> missingKeys = new ArrayList<>();
                for (Modular modular : Modular.values()) {
                    String moduleKey = UserLogTemplateKey.moduleName(modular.getValue());
                    if (!properties.containsKey(moduleKey)) {
                        missingKeys.add(moduleKey);
                    }
                }

                assertTrue(missingKeys.isEmpty(), templateFile + " missing module name templates: " + missingKeys);
            }
        }

        @Test
        void testEverySyncTypeCanMapToModular() {
            List<String> missingMappings = new ArrayList<>();
            for (SyncType syncType : SyncType.values()) {
                Modular modular = Modular.ofTaskSyncType(syncType.getValue());
                Modular expected = switch (syncType) {
                    case SYNC -> Modular.SYNC;
                    case MIGRATE -> Modular.MIGRATION;
                    case LOG_COLLECTOR -> Modular.LOG_COLLECTOR;
                    case CONN_HEARTBEAT -> Modular.CONN_HEARTBEAT;
                };
                if (expected != modular) {
                    missingMappings.add(syncType.getValue());
                }
            }

            assertTrue(missingMappings.isEmpty(),
                    "SyncType values must map to a valid Modular in Modular.ofTaskSyncType: " + missingMappings);
            assertEquals(Modular.MEM_CACHE, Modular.ofTaskSyncType(TaskDto.SYNC_TYPE_MEM_CACHE));
            assertEquals(Modular.MIGRATION, Modular.ofTaskSyncType(TaskDto.SYNC_TYPE_TEST_RUN));
            assertEquals(Modular.MIGRATION, Modular.ofTaskSyncType(TaskDto.SYNC_TYPE_PREVIEW));
            assertEquals(Modular.MIGRATION, Modular.ofTaskSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA));
            assertEquals(Modular.MIGRATION, Modular.ofTaskSyncType(null));
        }

        private Properties loadTemplate(String templateFile) throws IOException {
            Properties properties = new Properties();
            try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(templateFile)) {
                assertTrue(inputStream != null, "Template file not found: " + templateFile);
                properties.load(inputStream);
            }
            return properties;
        }

        private boolean hasSpecificOperationTemplate(Properties properties, String operation) {
            String internalPrefix = UserLogTemplateKey.PREFIX + "_";
            return properties.stringPropertyNames().stream()
                    .anyMatch(key -> key.startsWith(UserLogTemplateKey.PREFIX)
                            && !key.startsWith(internalPrefix)
                            && key.endsWith("." + operation));
        }
    }

}
